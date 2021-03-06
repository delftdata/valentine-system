import gzip
import json
import uuid
from timeit import default_timer
from typing import Union

from engine import app, celery
from engine.algorithms.algorithms import schema_only_algorithms
from engine.data_sources.postgres.postgres_source import PostgresSource
from engine.data_sources.postgres.postgres_table import PostgresTable
from engine.db import task_result_db, runtime_db, insertion_order_db, match_result_db, holistic_job_source_db
from engine.data_sources.atlas.atlas_source import AtlasSource
from engine.data_sources.atlas.atlas_table import AtlasTable
from engine.data_sources.minio.minio_source import MinioSource
from engine.data_sources.minio.minio_table import MinioTable
from engine.utils.api_utils import get_matcher, AtlasPayload, get_atlas_payload, get_atlas_source


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
    return task_uuid


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


@celery.task
def get_matches_holistic(job_id: str, matching_algorithm: str, algorithm_params: dict,
                         target_table: tuple, source_table: tuple):
    matcher = get_matcher(matching_algorithm, algorithm_params)
    target_db_name, target_table_name, t_source = target_table
    source_db_name, source_table_name, s_source = source_table
    load_data = False if matching_algorithm in schema_only_algorithms else True
    target_table = get_table(t_source, target_table_name, target_db_name, load_data=load_data)
    source_table = get_table(s_source, source_table_name, source_db_name, load_data=load_data)
    matches = matcher.get_matches(source_table, target_table)
    task_uuid: str = str(uuid.uuid4())
    task_result_db.set(task_uuid, gzip.compress(json.dumps(matches).encode('gbk')))
    holistic_job_source_db.set(job_id, json.dumps({"target": t_source, "source": s_source}))
    return task_uuid


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
    return task_uuid


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
def merge_matches(individual_match_uuids: list, job_uuid: str, start: float, max_number_of_matches: int = None):
    app.logger.info(f"Starting to merge results of job: {job_uuid}")
    start_merge = default_timer()
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
    end: float = default_timer()
    runtime: float = end - start
    app.logger.info(f"Starting to save results to db of job: {job_uuid}")
    start_store = default_timer()
    runtime_db.set(job_uuid, runtime)
    insertion_order_db.rpush('insertion_ordered_ids', job_uuid)
    match_result_db.set(job_uuid, gzip.compress(json.dumps(sorted_flattened_merged_matches).encode('gbk')))
    app.logger.info(f"job's : {job_uuid} results saved successfully in {default_timer() - start_store}")
    start_delete = default_timer()
    del sorted_flattened_merged_matches
    task_result_db.delete(*individual_match_uuids)
    app.logger.info(f"job's: {job_uuid} intermediate results deleted successfully in {default_timer() - start_delete}")
