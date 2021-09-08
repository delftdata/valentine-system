import gzip
import json
import uuid
from itertools import combinations
from timeit import default_timer
from typing import Union, Optional, Iterator

from more_itertools import unique_everseen
from urllib3.exceptions import ProtocolError

from engine import app, celery, ValentineLoadDataError
from engine.algorithms.algorithms import schema_only_algorithms
from engine.data_sources.postgres.postgres_source import PostgresSource
from engine.data_sources.postgres.postgres_table import PostgresTable
from engine.db import task_result_db, insertion_order_db, match_result_db, holistic_job_source_db, \
    holistic_jobs_completed_tasks_db, holistic_jobs_total_number_of_tasks_db, holistic_job_progression_counters, \
    holistic_job_uncompleted_tasks_queue
from engine.data_sources.atlas.atlas_source import AtlasSource
from engine.data_sources.atlas.atlas_table import AtlasTable
from engine.data_sources.minio.minio_source import MinioSource
from engine.data_sources.minio.minio_table import MinioTable
from engine.utils.api_utils import get_matcher, AtlasPayload, get_atlas_payload, get_atlas_source, validate_matcher


class NonSupportedBackend(Exception):
    pass


@celery.task
def get_matches_minio(matching_algorithm: str, algorithm_params: dict, target_table: tuple, source_table: tuple):
    matcher = get_matcher(matching_algorithm, algorithm_params)
    minio_source: MinioSource = MinioSource()
    target_db_name, target_table_name = target_table
    source_db_name, source_table_name = source_table
    load_data = False if matching_algorithm in schema_only_algorithms else True
    target_minio_table: MinioTable = minio_source.get_db_table(target_table_name,
                                                               target_db_name, load_data=load_data)
    source_minio_table: MinioTable = minio_source.get_db_table(source_table_name,
                                                               source_db_name, load_data=load_data)
    matches = matcher.get_matches(source_minio_table, target_minio_table)
    task_uuid: str = str(uuid.uuid4())
    task_result_db.set(task_uuid, gzip.compress(json.dumps(matches).encode('gbk')))


def get_table(source: str, table_name: str, db_name: str, load_data: bool) -> Union[PostgresTable, MinioTable]:
    if source == 'postgres':
        postgres_source: PostgresSource = PostgresSource()
        table: PostgresTable = postgres_source.get_db_table(table_name, db_name, load_data=load_data)
    elif source == 'minio':
        minio_source: MinioSource = MinioSource()
        table: MinioTable = minio_source.get_db_table(table_name, db_name, load_data=load_data)
    else:
        raise NonSupportedBackend
    return table


def get_deduplicated_table_combinations(tables) -> Iterator[tuple[tuple[str, str, str], tuple[str, str, str]]]:
    combs = combinations(tables, 2)
    return unique_everseen([((comb[0]['db_name'], comb[0]['table_name'], comb[0]['source']),
                             (comb[1]['db_name'], comb[1]['table_name'], comb[1]['source']))
                            for comb in combs
                            if comb[0] != comb[1]], key=frozenset)


@celery.task
def initiate_holistic_matching_job(job_uuid: str,
                                   algorithms: list[dict[str, Optional[dict[str, object]]]],
                                   tables):
    number_of_tasks: int = len(list(get_deduplicated_table_combinations(tables)))
    for algorithm in algorithms:
        algorithm_name, algorithm_params = list(algorithm.items())[0]
        validate_matcher(algorithm_name, algorithm_params, "minio_postgres")
        algorithm_uuid: str = job_uuid + "_" + algorithm_name
        holistic_jobs_total_number_of_tasks_db.set(algorithm_uuid, number_of_tasks)
        insertion_order_db.rpush('insertion_ordered_ids', algorithm_uuid)
        for table_combination in get_deduplicated_table_combinations(tables):
            task_uuid: str = str(uuid.uuid4())
            payload = json.dumps({'a1': algorithm_name,
                                  'a2': algorithm_params,
                                  't1': table_combination[0],
                                  't2': table_combination[1]})
            holistic_job_uncompleted_tasks_queue.hset(algorithm_uuid, key=task_uuid, value=payload)
        for task_id, payload in holistic_job_uncompleted_tasks_queue.hgetall(algorithm_uuid).items():
            payload = json.loads(payload)
            app.logger.info(f"Sending job: {algorithm_uuid} to Celery")
            get_matches_holistic.s(task_id, algorithm_uuid, payload['a1'], payload['a2'], payload['t1'],
                                   payload['t2']).apply_async()


@celery.task
def restart_failed_holistic_tasks(job_uuid: str):
    for task_id, payload in holistic_job_uncompleted_tasks_queue.hgetall(job_uuid).items():
        payload = json.loads(payload)
        app.logger.info(f"Restarting task: {task_id}  of job: {job_uuid}")
        get_matches_holistic.s(task_id, job_uuid, payload['a1'], payload['a2'], payload['t1'],
                               payload['t2']).apply_async()


@celery.task(autoretry_for=(ValentineLoadDataError, ProtocolError,),
             retry_kwargs={'max_retries': 5},
             default_retry_delay=5)
def get_matches_holistic(task_id: str, job_id: str, matching_algorithm: str, algorithm_params: dict,
                         target_table: tuple, source_table: tuple):
    matcher = get_matcher(matching_algorithm, algorithm_params)
    target_db_name, target_table_name, t_source = target_table
    source_db_name, source_table_name, s_source = source_table
    load_data = False if matching_algorithm in schema_only_algorithms else True
    app.logger.info(f'Retrieving tables {source_table_name} | {target_table_name} for job {job_id}')
    target_table = get_table(t_source, target_table_name, target_db_name, load_data=load_data)
    source_table = get_table(s_source, source_table_name, source_db_name, load_data=load_data)
    matches = matcher.get_matches(source_table, target_table)
    task_result_db.set(task_id, gzip.compress(json.dumps(matches).encode('gbk')))
    holistic_job_source_db.set(job_id, json.dumps({"target": t_source, "source": s_source}))
    holistic_jobs_completed_tasks_db.rpush(job_id, task_id)
    holistic_job_progression_counters.incr(job_id)
    holistic_job_uncompleted_tasks_queue.hdel(job_id, task_id)
    app.logger.info(f'Job {job_id} with tables {source_table_name} | {target_table_name} completed successfully ')


@celery.task
def get_matches_postgres(matching_algorithm: str, algorithm_params: dict, target_table: tuple, source_table: tuple):
    matcher = get_matcher(matching_algorithm, algorithm_params)
    postgres_source: PostgresSource = PostgresSource()
    target_db_name, target_table_name = target_table
    source_db_name, source_table_name = source_table
    load_data = False if matching_algorithm in schema_only_algorithms else True
    target_minio_table: PostgresTable = postgres_source.get_db_table(target_table_name,
                                                                     target_db_name, load_data=load_data)
    source_minio_table: PostgresTable = postgres_source.get_db_table(source_table_name,
                                                                     source_db_name, load_data=load_data)
    matches = matcher.get_matches(source_minio_table, target_minio_table)
    task_uuid: str = str(uuid.uuid4())
    task_result_db.set(task_uuid, gzip.compress(json.dumps(matches).encode('gbk')))


@celery.task
def get_matches_atlas(matching_algorithm: str, algorithm_params: dict, target_table_guid: str, source_table_guid: str,
                      rj):
    payload: AtlasPayload = get_atlas_payload(rj)
    matcher = get_matcher(matching_algorithm, algorithm_params)
    atlas_source: AtlasSource = get_atlas_source(payload)
    target_atlas_table: AtlasTable = atlas_source.get_db_table(target_table_guid)
    source_atlas_table: AtlasTable = atlas_source.get_db_table(source_table_guid)
    return matcher.get_matches(source_atlas_table, target_atlas_table)[:100]


@celery.task
def merge_matches(job_uuid: str, max_number_of_matches: int = None):
    app.logger.info(f"Starting to merge results of job: {job_uuid}")
    start_merge = default_timer()
    individual_match_uuids = holistic_jobs_completed_tasks_db.lrange(job_uuid, 0, -1)
    sorted_flattened_merged_matches = [item for sublist in
                                       [json.loads(gzip.decompress(task_result_db.get(task_uuid)))
                                        for task_uuid in individual_match_uuids]
                                       for item in sublist]
    app.logger.info(f"Merge results of job: {job_uuid} completed in {default_timer() - start_merge}")
    app.logger.info(f"Starting to sort results of job: {job_uuid}")
    start_sort = default_timer()
    sorted_flattened_merged_matches.sort(key=lambda k: k['sim'], reverse=True)
    app.logger.info(f"Sorting results of job: {job_uuid} completed in {default_timer() - start_sort}")
    if max_number_of_matches is not None:
        sorted_flattened_merged_matches = sorted_flattened_merged_matches[:max_number_of_matches]
    app.logger.info(f"Starting to save results to db of job: {job_uuid}")
    start_store = default_timer()
    match_result_db.set(job_uuid, gzip.compress(json.dumps(sorted_flattened_merged_matches).encode('gbk')))
    app.logger.info(f"job's : {job_uuid} results saved successfully in {default_timer() - start_store}")
    start_delete = default_timer()
    del sorted_flattened_merged_matches
    if len(individual_match_uuids) != 0:
        task_result_db.delete(*individual_match_uuids)
    app.logger.info(f"job's: {job_uuid} intermediate results deleted successfully in {default_timer() - start_delete}")
    holistic_jobs_completed_tasks_db.delete(job_uuid)
