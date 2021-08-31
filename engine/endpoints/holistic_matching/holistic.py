import uuid
from flask import Blueprint, request, jsonify

from engine import SYSTEM_RESERVED_MINIO_BUCKETS
from engine.data_sources.postgres.postgres_utils import list_postgres_dbs, list_postgres_db_tables
from engine.db import postgres_engine, minio_client
from engine.utils.api_utils import HolisticBulkPayload, get_holistic_bulk_payload
from engine.celery_tasks.holistic_matching import initiate_holistic_matching_job

app_matches_holistic = Blueprint('app_matches_holistic', __name__)


@app_matches_holistic.post('/matches/holistic/submit_batch_job')
def submit_holistic_batch_job():
    job_uuid: str = str(uuid.uuid4())
    payload: HolisticBulkPayload = get_holistic_bulk_payload(request.json)
    initiate_holistic_matching_job.s(job_uuid, payload.algorithms, payload.tables).apply_async()
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
