import uuid
from itertools import combinations, product
from timeit import default_timer
from typing import Iterator, Optional

from celery import chord
from flask import Blueprint, jsonify, request, abort
from minio.error import MinioException
from more_itertools import unique_everseen
from pandas.errors import EmptyDataError

from engine import app, SYSTEM_RESERVED_MINIO_BUCKETS
from engine.db import minio_client
from engine.data_sources.base_db import BaseDB
from engine.data_sources.base_source import GUIDMissing
from engine.data_sources.minio.minio_source import MinioSource
from engine.data_sources.minio.minio_table import MinioTable
from engine.utils.api_utils import validate_matcher, MinioBulkPayload, get_minio_bulk_payload, MinioPayload, \
    get_minio_payload
from engine.celery_tasks.holistic_matching import merge_matches, get_matches_minio


TABLE_DOES_NOT_EXIST_RESPONSE_STR = "The table does not exist"
EMPTY_TABLE_RESPONSE_STR = "The table does not contain any columns"

app_matches_minio = Blueprint('app_matches_minio', __name__)


@app_matches_minio.route("/matches/minio/holistic", methods=['POST'])
def find_holistic_matches_of_table_minio():
    payload: MinioPayload = get_minio_payload(request.json)
    validate_matcher(payload.matching_algorithm, payload.matching_algorithm_params, "minio")
    minio_source: MinioSource = MinioSource()
    try:
        dbs_tables_guids: list[list[str]] = [x.get_table_str_guids() for x in minio_source.get_all_dbs().values()]
        table: MinioTable = minio_source.get_db_table(payload.table_name, payload.db_name, load_data=False)
    except (GUIDMissing, MinioException):
        abort(400, TABLE_DOES_NOT_EXIST_RESPONSE_STR)
    except EmptyDataError:
        abort(400, EMPTY_TABLE_RESPONSE_STR)
    else:
        job_uuid: str = str(uuid.uuid4())
        callback = merge_matches.s(job_uuid, payload.max_number_matches)
        header = [get_matches_minio.s(payload.matching_algorithm, payload.matching_algorithm_params, *table_combination)
                  for table_combination in
                  product([item for sublist in dbs_tables_guids for item in sublist], [table.unique_identifier])]
        chord(header)(callback)
        return jsonify(job_uuid)


@app_matches_minio.route('/matches/minio/other_db/<db_name>', methods=['POST'])
def find_matches_other_db_minio(db_name: str):
    payload: MinioPayload = get_minio_payload(request.json)
    validate_matcher(payload.matching_algorithm, payload.matching_algorithm_params, "minio")
    minio_source: MinioSource = MinioSource()
    if not minio_source.contains_db(payload.db_name):
        abort(400, "The source does not contain the given database")
    try:
        db: BaseDB = minio_source.get_db(db_name, load_data=False)
        if db.is_empty:
            abort(400, "The given db does not contain any tables")
        table: MinioTable = minio_source.get_db_table(payload.table_name, payload.db_name, load_data=False)
    except (GUIDMissing, MinioException):
        abort(400, TABLE_DOES_NOT_EXIST_RESPONSE_STR)
    except EmptyDataError:
        abort(400, EMPTY_TABLE_RESPONSE_STR)
    else:
        job_uuid: str = str(uuid.uuid4())
        callback = merge_matches.s(job_uuid, payload.max_number_matches)
        header = [get_matches_minio.s(payload.matching_algorithm, payload.matching_algorithm_params, *table_combination)
                  for table_combination in product(db.get_table_str_guids(), [table.unique_identifier])]
        chord(header)(callback)
        return jsonify(job_uuid)


@app_matches_minio.route('/matches/minio/within_db', methods=['POST'])
def find_matches_within_db_minio():
    payload: MinioPayload = get_minio_payload(request.json)
    validate_matcher(payload.matching_algorithm, payload.matching_algorithm_params, "minio")
    minio_source: MinioSource = MinioSource()
    if not minio_source.contains_db(payload.db_name):
        abort(400, "The source does not contain the given database")
    try:
        db: BaseDB = minio_source.get_db(payload.db_name, load_data=False)
        if db.is_empty:
            abort(400, "The given db does not contain any tables")
        if db.number_of_tables == 1:
            abort(400, "The given db only contains one table")
        table: MinioTable = minio_source.get_db_table(payload.table_name, payload.db_name, load_data=False)
    except (GUIDMissing, MinioException):
        abort(400, TABLE_DOES_NOT_EXIST_RESPONSE_STR)
    except EmptyDataError:
        abort(400, "The table does not contain any columns")
    else:
        db.remove_table(payload.table_name)
        job_uuid: str = str(uuid.uuid4())
        callback = merge_matches.s(job_uuid, payload.max_number_matches)
        header = [get_matches_minio.s(payload.matching_algorithm, payload.matching_algorithm_params, *table_combination)
                  for table_combination in product(db.get_table_str_guids(), [table.unique_identifier])]
        chord(header)(callback)
        return jsonify(job_uuid)


@app_matches_minio.route('/matches/minio/submit_batch_job', methods=['POST'])
def submit_batch_job():
    job_uuid: str = str(uuid.uuid4())

    payload: MinioBulkPayload = get_minio_bulk_payload(request.json)
    app.logger.info(f"Retrieving data for job: {job_uuid}")

    algorithm: dict[str, Optional[dict[str, object]]]
    for algorithm in payload.algorithms:
        algorithm_name, algorithm_params = list(algorithm.items())[0]
        algorithm_uuid: str = job_uuid + "_" + algorithm_name
        validate_matcher(algorithm_name, algorithm_params, "minio")
        app.logger.info(f"Sending job: {algorithm_uuid} to Celery")

        combs = combinations(payload.tables, 2)

        deduplicated_table_combinations: Iterator[tuple[tuple[str, str], tuple[str, str]]] = unique_everseen([
            ((comb[0]['db_name'], comb[0]['table_name']), (comb[1]['db_name'], comb[1]['table_name']))
            for comb in combs
            if comb[0] != comb[1]], key=frozenset)

        start = default_timer()
        callback = merge_matches.s(algorithm_uuid, start)
        header = [get_matches_minio.s(algorithm_name, algorithm_params, *table_combination)
                  for table_combination in deduplicated_table_combinations]
        chord(header)(callback)

    return jsonify(job_uuid)


@app_matches_minio.route('/matches/minio/ls', methods=['GET'])
def get_minio_dir_tree():
    return jsonify([{"db_name": bucket.name,
                     "tables": [obj.object_name for obj in minio_client.list_objects(bucket.name)]
                     }
                    for bucket in minio_client.list_buckets() if bucket.name not in SYSTEM_RESERVED_MINIO_BUCKETS])


@app_matches_minio.route('/matches/minio/column_sample/<db_name>/<table_name>/<column_name>', methods=['GET'])
def get_column_sample_minio(db_name: str, table_name: str, column_name: str):
    minio_source: MinioSource = MinioSource()
    return jsonify(minio_source.get_column_sample(db_name, table_name, column_name))
