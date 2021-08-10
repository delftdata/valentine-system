import uuid
from itertools import product

from celery import chord
from flask import Blueprint, jsonify, request, abort
from pandas.errors import EmptyDataError

from engine.data_sources.base_db import BaseDB
from engine.data_sources.base_source import GUIDMissing
from engine.data_sources.postgres.postgres_source import PostgresSource
from engine.data_sources.postgres.postgres_table import PostgresTable
from engine.data_sources.postgres.postgres_utils import list_postgres_dbs, list_postgres_db_tables, \
    get_column_sample_from_postgres_table
from engine.db import postgres_engine
from engine.utils.api_utils import validate_matcher, PostgresPayload, get_postgres_payload
from engine.celery_tasks.holistic_matching import merge_matches, get_matches_postgres


TABLE_DOES_NOT_EXIST_RESPONSE_STR = "The table does not exist"
EMPTY_TABLE_RESPONSE_STR = "The table does not contain any columns"

app_matches_postgres = Blueprint('app_matches_postgres', __name__)


@app_matches_postgres.post("/matches/postgres/holistic")
def find_holistic_matches_of_table_postgres():
    payload: PostgresPayload = get_postgres_payload(request.json)
    validate_matcher(payload.matching_algorithm, payload.matching_algorithm_params, "postgres")
    postgres_source: PostgresSource = PostgresSource()
    try:
        dbs_tables_guids: list[list[str]] = [x.get_table_str_guids() for x in postgres_source.get_all_dbs().values()]
        table: PostgresTable = postgres_source.get_db_table(payload.table_name, payload.db_name, load_data=False)
    except GUIDMissing:
        abort(400, TABLE_DOES_NOT_EXIST_RESPONSE_STR)
    except EmptyDataError:
        abort(400, EMPTY_TABLE_RESPONSE_STR)
    else:
        job_uuid: str = str(uuid.uuid4())
        callback = merge_matches.s(job_uuid, payload.max_number_matches)
        header = [get_matches_postgres.s(payload.matching_algorithm,
                                         payload.matching_algorithm_params, *table_combination)
                  for table_combination in
                  product([item for sublist in dbs_tables_guids for item in sublist], [table.unique_identifier])]
        chord(header)(callback)
        return jsonify(job_uuid)


@app_matches_postgres.post('/matches/postgres/other_db/<db_name>')
def find_matches_other_db_postgres(db_name: str):
    payload: PostgresPayload = get_postgres_payload(request.json)
    validate_matcher(payload.matching_algorithm, payload.matching_algorithm_params, "postgres")
    postgres_source: PostgresSource = PostgresSource()
    if not postgres_source.contains_db(payload.db_name):
        abort(400, "The source does not contain the given database")
    try:
        db: BaseDB = postgres_source.get_db(db_name, load_data=False)
        if db.is_empty:
            abort(400, "The given db does not contain any tables")
        table: PostgresTable = postgres_source.get_db_table(payload.table_name, payload.db_name, load_data=False)
    except GUIDMissing:
        abort(400, TABLE_DOES_NOT_EXIST_RESPONSE_STR)
    except EmptyDataError:
        abort(400, EMPTY_TABLE_RESPONSE_STR)
    else:
        job_uuid: str = str(uuid.uuid4())
        callback = merge_matches.s(job_uuid, payload.max_number_matches)
        header = [get_matches_postgres.s(payload.matching_algorithm,
                                         payload.matching_algorithm_params, *table_combination)
                  for table_combination in product(db.get_table_str_guids(), [table.unique_identifier])]
        chord(header)(callback)
        return jsonify(job_uuid)


@app_matches_postgres.post('/matches/postgres/within_db')
def find_matches_within_db_postgres():
    payload: PostgresPayload = get_postgres_payload(request.json)
    validate_matcher(payload.matching_algorithm, payload.matching_algorithm_params, "postgres")
    postgres_source: PostgresSource = PostgresSource()
    if not postgres_source.contains_db(payload.db_name):
        abort(400, "The source does not contain the given database")
    try:
        db: BaseDB = postgres_source.get_db(payload.db_name, load_data=False)
        if db.is_empty:
            abort(400, "The given db does not contain any tables")
        if db.number_of_tables == 1:
            abort(400, "The given db only contains one table")
        table: PostgresTable = postgres_source.get_db_table(payload.table_name, payload.db_name, load_data=False)
    except GUIDMissing:
        abort(400, TABLE_DOES_NOT_EXIST_RESPONSE_STR)
    except EmptyDataError:
        abort(400, "The table does not contain any columns")
    else:
        db.remove_table(payload.table_name)
        job_uuid: str = str(uuid.uuid4())
        callback = merge_matches.s(job_uuid, payload.max_number_matches)
        header = [get_matches_postgres.s(payload.matching_algorithm,
                                         payload.matching_algorithm_params, *table_combination)
                  for table_combination in product(db.get_table_str_guids(), [table.unique_identifier])]
        chord(header)(callback)
        return jsonify(job_uuid)


@app_matches_postgres.get('/matches/postgres/ls')
def get_postgres_database_tree():
    result = []
    for database in list_postgres_dbs(postgres_url=postgres_engine.url):
        tables = list_postgres_db_tables(postgres_engine.url, database)
        if len(tables) > 0:
            result.append({"db_name": database, "tables": tables})
    return jsonify(result)


@app_matches_postgres.get('/matches/postgres/column_sample/<db_name>/<table_name>/<column_name>')
def get_column_sample_postgres(db_name: str, table_name: str, column_name: str):
    sample = get_column_sample_from_postgres_table(postgres_engine.url, db_name, table_name, column_name, n=10)
    return jsonify(sample)
