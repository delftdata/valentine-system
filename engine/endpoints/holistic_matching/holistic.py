import uuid
from itertools import combinations
from timeit import default_timer
from typing import Optional, Iterator

from celery import chord
from flask import Blueprint, request, jsonify
from more_itertools import unique_everseen

from engine import app, SYSTEM_RESERVED_MINIO_BUCKETS
from engine.data_sources.postgres.postgres_utils import list_postgres_dbs, list_postgres_db_tables
from engine.db import postgres_engine, minio_client
from engine.utils.api_utils import HolisticBulkPayload, get_holistic_bulk_payload, validate_matcher
from engine.celery_tasks.holistic_matching import merge_matches, get_matches_holistic

app_matches_holistic = Blueprint('app_matches_holistic', __name__)


@app_matches_holistic.post('/matches/holistic/submit_batch_job')
def submit_holistic_batch_job():
    job_uuid: str = str(uuid.uuid4())

    payload: HolisticBulkPayload = get_holistic_bulk_payload(request.json)
    app.logger.info(f"Retrieving data for job: {job_uuid}")
    algorithm: dict[str, Optional[dict[str, object]]]
    for algorithm in payload.algorithms:
        algorithm_name, algorithm_params = list(algorithm.items())[0]
        algorithm_uuid: str = job_uuid + "_" + algorithm_name
        validate_matcher(algorithm_name, algorithm_params, "minio_postgres")
        app.logger.info(f"Sending job: {algorithm_uuid} to Celery")

        combs = combinations(payload.tables, 2)

        deduplicated_table_combinations: Iterator[tuple[tuple[str, str, str], tuple[str, str, str]]] = unique_everseen([
            ((comb[0]['db_name'], comb[0]['table_name'], comb[0]['source']),
             (comb[1]['db_name'], comb[1]['table_name'], comb[1]['source']))
            for comb in combs
            if comb[0] != comb[1]], key=frozenset)

        start = default_timer()
        callback = merge_matches.s(algorithm_uuid, start)
        header = [get_matches_holistic.s(algorithm_uuid, algorithm_name, algorithm_params, *table_combination)
                  for table_combination in deduplicated_table_combinations]
        chord(header)(callback)

    return jsonify(job_uuid)


@app_matches_holistic.get('/matches/holistic/ls_tables')
def matches_holistic_ls_tables():
    postgres_tables = []
    for database in list_postgres_dbs(postgres_url=postgres_engine.url):
        tables = list_postgres_db_tables(postgres_engine.url, database)
        if len(tables) > 0:
            postgres_tables.append({"db_name": database, "tables": tables})
    minio_tables = [{"db_name": bucket.name,
                     "tables": [obj.object_name for obj in minio_client.list_objects(bucket.name)]
                     }
                    for bucket in minio_client.list_buckets() if bucket.name not in SYSTEM_RESERVED_MINIO_BUCKETS]
    result = {"postgres": postgres_tables,
              "minio": minio_tables}
    return jsonify(result)
