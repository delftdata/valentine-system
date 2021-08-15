import os
from tempfile import gettempdir

import pandas as pd
from flask import Blueprint, Response, abort, jsonify
from sqlalchemy import create_engine
from sqlalchemy_utils import database_exists, create_database
from werkzeug.utils import secure_filename

from engine.data_sources.postgres.postgres_utils import list_postgres_dbs, list_postgres_db_tables, \
    DatabaseDoesNotExist, get_columns_from_postgres_table, get_pandas_df_from_postgres_table
from engine.db import postgres_engine
from engine.forms import UploadFileForm
from engine.utils.utils import get_encoding, get_delimiter

TAKEN_DB_NAMES = ["postgres", "template0", "template1"]


app_postgres_utils = Blueprint('app_postgres_utils', __name__)


@app_postgres_utils.get('/postgres/get_databases')
def postgres_get_databases():
    return jsonify(list_postgres_dbs(postgres_engine.url))


@app_postgres_utils.post('/postgres/create_database/<db_name>')
def postgres_create_database(db_name: str):
    engine = create_engine(str(postgres_engine.url) + '/' + db_name)
    if not database_exists(engine.url):
        create_database(engine.url)
    else:
        abort(400, f"DB: {db_name} already exists")
    return Response(f"DB: {db_name} created successfully", status=200)


@app_postgres_utils.get('/postgres/get_tables_of_db/<db_name>')
def postgres_get_tables_of_db(db_name: str):
    try:
        tables = list_postgres_db_tables(postgres_engine.url, db_name)
    except DatabaseDoesNotExist:
        abort(400, "Database does not exist")
    else:
        return jsonify(tables)


@app_postgres_utils.post('/postgres/store_csv_as_table/<db_name>/<table_name>')
def postgres_store_csv_as_table(db_name: str, table_name: str):
    form = UploadFileForm()
    if not form.validate_on_submit():
        abort(400, 'File not included in the submission form')
    tmp_dir: str = gettempdir()
    filename = secure_filename(form.resource.data.filename)
    src_file = os.path.join(tmp_dir, filename)
    form.resource.data.save(src_file)
    engine = create_engine(str(postgres_engine.url) + '/' + db_name)
    if not database_exists(engine.url):
        abort(400, "Database does not exist")
    df = pd.read_csv(src_file,
                     index_col=False,
                     encoding=get_encoding(src_file),
                     sep=get_delimiter(src_file),
                     on_bad_lines='warn')
    df.to_sql(table_name,
              con=engine,
              if_exists='replace',
              index=False)
    os.remove(src_file)
    return Response(f"Table: {table_name} created successfully in DB: {db_name} from csv file {filename}", status=200)


@app_postgres_utils.get('/postgres/get_table_columns/<db_name>/<table_name>')
def postgres_get_table_columns(db_name: str, table_name: str):
    columns = get_columns_from_postgres_table(postgres_engine.url, db_name, table_name)
    return jsonify(columns)


@app_postgres_utils.get('/postgres/get_table/<db_name>/<table_name>')
def postgres_get_table(db_name: str, table_name: str):
    table_dict = get_pandas_df_from_postgres_table(postgres_engine.url, db_name, table_name).to_dict()
    return jsonify(table_dict)
