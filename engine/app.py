import json
import os
import uuid
import logging
import gzip
from io import BytesIO
from os import path
from tempfile import gettempdir
from time import sleep
from timeit import default_timer

from celery import Celery, chord
from minio import Minio
from minio.error import MinioException
from flask import Flask, request, abort, jsonify, Response
from flask_cors import CORS
from typing import List, Dict, Optional, Tuple, Iterator
from itertools import product, combinations

from more_itertools import unique_everseen
from pandas.errors import EmptyDataError
from redis import Redis
from werkzeug.utils import secure_filename

from engine.algorithms.algorithms import schema_only_algorithms
from engine.data_sources.atlas.atlas_table import AtlasTable
from engine.data_sources.base_source import GUIDMissing
from engine.data_sources.atlas.atlas_source import AtlasSource
from engine.data_sources.base_db import BaseDB
from engine.data_sources.minio.minio_source import MinioSource
from engine.data_sources.minio.minio_table import MinioTable
from engine.forms import UploadFileToMinioForm, DatasetFabricationForm
from engine.utils.api_utils import AtlasPayload, get_atlas_payload, validate_matcher, get_atlas_source, get_matcher, \
    MinioPayload, get_minio_payload, get_minio_bulk_payload, MinioBulkPayload

app = Flask(__name__)
CORS(app)

app.config['CELERY_BROKER_URL'] = 'amqp://{user}:{pwd}@{host}:{port}/'.format(user=os.environ['RABBITMQ_DEFAULT_USER'],
                                                                              pwd=os.environ['RABBITMQ_DEFAULT_PASS'],
                                                                              host=os.environ['RABBITMQ_HOST'],
                                                                              port=os.environ['RABBITMQ_PORT'])

app.config['CELERY_RESULT_BACKEND_URL'] = 'redis://:{password}@{host}:{port}/0'.format(host=os.environ['REDIS_HOST'],
                                                                                       port=os.environ['REDIS_PORT'],
                                                                                       password=os.environ['REDIS_PASSWORD'])

celery = Celery(app.name, broker=app.config['CELERY_BROKER_URL'], backend=app.config['CELERY_RESULT_BACKEND_URL'])
celery.conf.update(app.config)
celery.conf.update(task_serializer='msgpack',
                   accept_content=['msgpack'],
                   result_serializer='msgpack',
                   task_acks_late=True,
                   worker_prefetch_multiplier=1
                   )

match_result_db: Redis = Redis(host=os.environ['REDIS_HOST'], port=os.environ['REDIS_PORT'], password=os.environ['REDIS_PASSWORD'],
                               db=1)
insertion_order_db: Redis = Redis(host=os.environ['REDIS_HOST'], port=os.environ['REDIS_PORT'], password=os.environ['REDIS_PASSWORD'],
                                  charset="utf-8", decode_responses=True, db=2)
verified_match_db: Redis = Redis(host=os.environ['REDIS_HOST'], port=os.environ['REDIS_PORT'], password=os.environ['REDIS_PASSWORD'],
                                 charset="utf-8", decode_responses=True, db=3)
runtime_db: Redis = Redis(host=os.environ['REDIS_HOST'], port=os.environ['REDIS_PORT'], password=os.environ['REDIS_PASSWORD'],
                          charset="utf-8", decode_responses=True, db=4)
task_result_db: Redis = Redis(host=os.environ['REDIS_HOST'], port=os.environ['REDIS_PORT'], password=os.environ['REDIS_PASSWORD'],
                              db=5)

minio_client: Minio = Minio('{host}:{port}'.format(host=os.environ['MINIO_HOST'],
                                                   port=os.environ['MINIO_PORT']),
                            access_key=os.environ['MINIO_ACCESS_KEY'],
                            secret_key=os.environ['MINIO_SECRET_KEY'],
                            secure=False)


@celery.task
def get_matches_minio(matching_algorithm: str, algorithm_params: dict, target_table: tuple, source_table: tuple):
    matcher = get_matcher(matching_algorithm, algorithm_params)
    minio_source: MinioSource = MinioSource()
    target_db_name, target_table_name = target_table
    source_db_name, source_table_name = source_table
    load_data = False if matching_algorithm in schema_only_algorithms else True
    target_minio_table: MinioTable = minio_source.get_db_table(target_table_name, target_db_name, load_data=load_data)
    source_minio_table: MinioTable = minio_source.get_db_table(source_table_name, source_db_name, load_data=load_data)
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


@app.route("/matches/atlas/holistic/<table_guid>", methods=['POST'])
def find_holistic_matches_of_table_atlas(table_guid: str):
    payload: AtlasPayload = get_atlas_payload(request.json)
    validate_matcher(payload.matching_algorithm, payload.matching_algorithm_params, "atlas")
    atlas_src: AtlasSource = get_atlas_source(payload)
    try:
        table: AtlasTable = atlas_src.get_db_table(table_guid)
        if table.is_empty:
            abort(400, "The table does not have any columns")
        dbs_tables_guids: List[List[str]] = [x.get_table_str_guids() for x in atlas_src.get_all_dbs().values()]
    except json.JSONDecodeError:
        abort(500, "Couldn't connect to Atlas. This is a network issue, "
                   "try to lower the request_chunk_size and the request_parallelism in the payload")
    except GUIDMissing:
        abort(400, 'This guid does not correspond to any database in atlas! '
                   'Check if the given database types are correct or if there is a mistake in the guid')
    except KeyError:
        abort(400, 'This guid does not correspond to any table in atlas! '
                   'Check if the given table types are correct or if there is a mistake in the guid')
    else:
        job_uuid: str = str(uuid.uuid4())
        callback = merge_matches.s(job_uuid, payload.max_number_matches)
        header = [get_matches_atlas.s(payload.matching_algorithm, payload.matching_algorithm_params, *table_combination,
                                      request.json)
                  for table_combination in
                  product([item for sublist in dbs_tables_guids for item in sublist], [table.unique_identifier])]
        chord(header)(callback)
        return jsonify(job_uuid)


@app.route('/matches/atlas/other_db/<table_guid>/<db_guid>', methods=['POST'])
def find_matches_other_db_atlas(table_guid: str, db_guid: str):
    payload: AtlasPayload = get_atlas_payload(request.json)
    validate_matcher(payload.matching_algorithm, payload.matching_algorithm_params, "atlas")
    atlas_src: AtlasSource = get_atlas_source(payload)
    try:
        table: AtlasTable = atlas_src.get_db_table(table_guid)
        if table.is_empty:
            abort(400, "The table does not have any columns")
        db: BaseDB = atlas_src.get_db(db_guid)
    except json.JSONDecodeError:
        abort(500, "Couldn't connect to Atlas. This is a network issue, "
                   "try to lower the request_chunk_size and the request_parallelism in the payload")
    except GUIDMissing:
        abort(400, 'This guid does not correspond to any database in atlas! '
                   'Check if the given database types are correct or if there is a mistake in the guid')
    except KeyError:
        abort(400, 'This guid does not correspond to any table in atlas! '
                   'Check if the given table types are correct or if there is a mistake in the guid')
    else:
        job_uuid: str = str(uuid.uuid4())
        callback = merge_matches.s(job_uuid, payload.max_number_matches)
        header = [get_matches_atlas.s(payload.matching_algorithm, payload.matching_algorithm_params, *table_combination,
                                      request.json)
                  for table_combination in product(db.get_table_str_guids(), [table.unique_identifier])]
        chord(header)(callback)
        return jsonify(job_uuid)


@app.route('/matches/atlas/within_db/<table_guid>', methods=['POST'])
def find_matches_within_db_atlas(table_guid: str):
    payload: AtlasPayload = get_atlas_payload(request.json)
    validate_matcher(payload.matching_algorithm, payload.matching_algorithm_params, "atlas")
    atlas_src: AtlasSource = get_atlas_source(payload)
    try:
        table: AtlasTable = atlas_src.get_db_table(table_guid)
        if table.is_empty:
            abort(400, "The table does not have any columns")
        db: BaseDB = atlas_src.get_db(table.db_belongs_uid)
        if db.number_of_tables == 1:
            abort(400, "The given db only contains one table")
    except json.JSONDecodeError:
        abort(500, "Couldn't connect to Atlas. This is a network issue, "
                   "try to lower the request_chunk_size and the request_parallelism in the payload")
    except GUIDMissing:
        abort(400, 'This guid does not correspond to any database in atlas! '
                   'Check if the given database types are correct or if there is a mistake in the guid')
    except KeyError:
        abort(400, 'This guid does not correspond to any table in atlas! '
                   'Check if the given table types are correct or if there is a mistake in the guid')
    else:
        # remove the table from the schema so that it doesn't compare against itself
        db.remove_table(table_guid)
        job_uuid: str = str(uuid.uuid4())
        callback = merge_matches.s(job_uuid, payload.max_number_matches)
        header = [get_matches_atlas.s(payload.matching_algorithm, payload.matching_algorithm_params, *table_combination,
                                      request.json)
                  for table_combination in product(db.get_table_str_guids(), [table.unique_identifier])]
        chord(header)(callback)
        return jsonify(job_uuid)


@app.route("/matches/minio/holistic", methods=['POST'])
def find_holistic_matches_of_table_minio():
    payload: MinioPayload = get_minio_payload(request.json)
    validate_matcher(payload.matching_algorithm, payload.matching_algorithm_params, "minio")
    minio_source: MinioSource = MinioSource()
    try:
        dbs_tables_guids: List[List[str]] = [x.get_table_str_guids() for x in minio_source.get_all_dbs().values()]
        table: MinioTable = minio_source.get_db_table(payload.table_name, payload.db_name, load_data=False)
    except (GUIDMissing, MinioException):
        abort(400, "The table does not exist")
    except EmptyDataError:
        abort(400, "The table does not contain any columns")
    else:
        job_uuid: str = str(uuid.uuid4())
        callback = merge_matches.s(job_uuid, payload.max_number_matches)
        header = [get_matches_minio.s(payload.matching_algorithm, payload.matching_algorithm_params, *table_combination)
                  for table_combination in
                  product([item for sublist in dbs_tables_guids for item in sublist], [table.unique_identifier])]
        chord(header)(callback)
        return jsonify(job_uuid)


@app.route('/matches/minio/other_db/<db_name>', methods=['POST'])
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
        abort(400, "The table does not exist")
    except EmptyDataError:
        abort(400, "The table does not contain any columns")
    else:
        job_uuid: str = str(uuid.uuid4())
        callback = merge_matches.s(job_uuid, payload.max_number_matches)
        header = [get_matches_minio.s(payload.matching_algorithm, payload.matching_algorithm_params, *table_combination)
                  for table_combination in product(db.get_table_str_guids(), [table.unique_identifier])]
        chord(header)(callback)
        return jsonify(job_uuid)


@app.route('/matches/minio/within_db', methods=['POST'])
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
        abort(400, "The table does not exist")
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


@app.route('/matches/minio/submit_batch_job', methods=['POST'])
def submit_batch_job():
    job_uuid: str = str(uuid.uuid4())

    payload: MinioBulkPayload = get_minio_bulk_payload(request.json)
    app.logger.info(f"Retrieving data for job: {job_uuid}")

    algorithm: Dict[str, Optional[Dict[str, object]]]
    for algorithm in payload.algorithms:
        algorithm_name, algorithm_params = list(algorithm.items())[0]
        algorithm_uuid: str = job_uuid + "_" + algorithm_name
        validate_matcher(algorithm_name, algorithm_params, "minio")
        app.logger.info(f"Sending job: {algorithm_uuid} to Celery")

        combs = combinations(payload.tables, 2)

        deduplicated_table_combinations: Iterator[Tuple[Tuple[str, str], Tuple[str, str]]] = unique_everseen([
            ((comb[0]['db_name'], comb[0]['table_name']), (comb[1]['db_name'], comb[1]['table_name']))
            for comb in combs
            if comb[0] != comb[1]], key=frozenset)

        start = default_timer()
        callback = merge_matches.s(algorithm_uuid, start)
        header = [get_matches_minio.s(algorithm_name, algorithm_params, *table_combination)
                  for table_combination in deduplicated_table_combinations]
        chord(header)(callback)

    return jsonify(job_uuid)


@app.route('/matches/minio/ls', methods=['GET'])
def get_minio_dir_tree():
    return jsonify([{"db_name": bucket.name,
                     "tables": [obj.object_name for obj in minio_client.list_objects(bucket.name)]
                     }
                    for bucket in minio_client.list_buckets()])


@app.route('/matches/minio/column_sample/<db_name>/<table_name>/<column_name>', methods=['GET'])
def get_column_sample_minio(db_name: str, table_name: str, column_name: str):
    minio_source: MinioSource = MinioSource()
    return jsonify(minio_source.get_column_sample(db_name, table_name, column_name))


@app.route('/results/finished_jobs', methods=['GET'])
def get_finished_jobs():
    return jsonify(insertion_order_db.lrange('insertion_ordered_ids', 0, -1))


@app.route('/results/job_results/<job_id>', methods=['GET'])
def get_job_results(job_id: str):
    results = json.loads(gzip.decompress(match_result_db.get(job_id)))
    if results is None:
        return Response("Job does not exist", status=400)
    return jsonify(results)


@app.route('/results/job_runtime/<job_id>', methods=['GET'])
def get_job_runtime(job_id: str):
    results = runtime_db.get(job_id)
    if results is None:
        return Response("Job does not exist", status=400)
    return jsonify(json.loads(results))


@app.route('/results/save_verified_match/<job_id>/<index>', methods=['POST'])
def save_verified_match(job_id: str, index: int):
    results = match_result_db.get(job_id)
    if results is None:
        return Response("Job does not exist", status=400)
    ranked_list: list = json.loads(gzip.decompress(results))
    try:
        to_save = ranked_list.pop(int(index))
    except IndexError:
        return Response("Match does not exist", status=400)
    verified_matches = [json.loads(x) for x in verified_match_db.lrange('verified_matches', 0, -1)]
    match_result_db.set(job_id, gzip.compress(json.dumps(ranked_list).encode('gbk')))
    if to_save in verified_matches:
        return Response("Match already verified", status=200)
    verified_match_db.rpush('verified_matches', json.dumps(to_save))
    return Response("Matched saved successfully", status=200)


@app.route('/results/discard_match/<job_id>/<index>', methods=['POST'])
def discard_match(job_id: str, index: int):
    results = match_result_db.get(job_id)
    if results is None:
        return Response("Job does not exist", status=400)
    ranked_list: list = json.loads(gzip.decompress(results))
    try:
        ranked_list.pop(int(index))
    except IndexError:
        return Response("Match does not exist", status=400)
    match_result_db.set(job_id, gzip.compress(json.dumps(ranked_list).encode('gbk')))
    return Response("Matched discarded successfully", status=200)


@app.route('/results/delete_job/<job_id>', methods=['POST'])
def delete_job(job_id: str):
    match_result_db.delete(job_id)
    insertion_order_db.lrem('insertion_ordered_ids', 1, job_id)
    return Response("Job discarded successfully", status=200)


@app.route('/results/verified_matches', methods=['GET'])
def get_verified_matches():
    verified_matches = [json.loads(x) for x in verified_match_db.lrange('verified_matches', 0, -1)]
    return jsonify(verified_matches)


@app.route('/minio/create_bucket/<bucket_name>', methods=['POST'])
def create_minio_bucket(bucket_name: str):
    minio_client.make_bucket(bucket_name)
    return Response(f"Bucket {bucket_name} created successfully", status=200)


@app.route('/minio/upload_file/<bucket_name>', methods=['POST'])
def minio_upload_file(bucket_name: str):
    form = UploadFileToMinioForm()
    if not form.validate_on_submit():
        abort(400, form.errors)
    tmp_dir: str = gettempdir()
    filename = secure_filename(form.resource.data.filename)
    src_file = path.join(tmp_dir, filename)
    form.resource.data.save(src_file)
    minio_client.fput_object(bucket_name, filename, src_file)
    return Response(f"File {filename} uploaded in bucket {bucket_name} successfully", status=200)


@app.route('/valentine/results/get_fabricated_data', methods=['GET'])
def valentine_get_fabricated_data():
    fabricated_datasets = {
        "miller": ["miller_both_0_1_ac1_av", "miller_both_50_70_ac4_av", "miller_both_0_1_ac1_ev",
                    "miller_both_50_70_ac4_ev", "miller_both_0_1_ac2_av", "miller_both_50_70_ac5_av", "miller_both_0_1_ac2_ev",
                    "miller_both_50_70_ac5_ev", "miller_both_0_1_ac3_av", "miller_both_50_70_ec_av", "miller_both_0_1_ac3_ev",
                    "miller_both_50_70_ec_ev", "miller_both_0_1_ac4_av", "miller_horizontal_0_ac1_av", "miller_both_0_1_ac4_ev",
                    "miller_horizontal_0_ac1_ev", "miller_both_0_1_ac5_av", "miller_horizontal_0_ac2_av", "miller_both_0_1_ac5_ev",
                    "miller_horizontal_0_ac2_ev", "miller_both_0_1_ec_av", "miller_horizontal_0_ac3_av", "miller_both_0_1_ec_ev",
                    "miller_horizontal_0_ac3_ev", "miller_both_0_30_ac1_av", "miller_horizontal_0_ac4_av", "miller_both_0_30_ac1_ev",
                    "miller_horizontal_0_ac4_ev", "miller_both_0_30_ac2_av", "miller_horizontal_0_ac5_av", "miller_both_0_30_ac2_ev",
                    "miller_horizontal_0_ac5_ev", "miller_both_0_30_ac3_av", "miller_horizontal_0_ec_av", "miller_both_0_30_ac3_ev",
                    "miller_horizontal_0_ec_ev", "miller_both_0_30_ac4_av", "miller_horizontal_100_ac1_av", "miller_both_0_30_ac4_ev",
                    "miller_horizontal_100_ac1_ev"],
    }
    return jsonify(fabricated_datasets)


@app.route('/valentine/results/delete_fabricated_dataset/<dataset_id>', methods=['GET'])
def valentine_delete_fabricated_dataset(dataset_id: str):
    sleep(0.1)
    return Response(f"Dataset {dataset_id} deleted successfully", status=200)


@app.route('/valentine/results/download_fabricated_dataset/<dataset_id>', methods=['GET'])
def valentine_download_fabricated_dataset(dataset_id: str):
    obj_size = minio_client.stat_object('VaLeNtInE', 'output.zip').size
    data = BytesIO(list(minio_client.get_object('VaLeNtInE', 'output.zip').stream(obj_size))[0])
    return Response(data, headers={'Content-Type': 'application/zip',
                                   'Content-Disposition': 'attachment; filename=%s;' % dataset_id
                                   }, status=200)


@app.route('/valentine/upload_dataset', methods=['POST'])
def valentine_upload_dataset():
    try:
        uploaded_file = request.files["file"]
        if uploaded_file:
            size = os.fstat(uploaded_file.fileno()).st_size
            minio_client.put_object('VaLeNtInE', uploaded_file.filename, uploaded_file, size)
    except Exception:
        abort(400, "File Could not be uploaded")
    else:
        return Response("object stored in bucket", status=200)


@app.route('/valentine/get_fabricated_sample/<dataset_id>', methods=['GET'])
def valentine_get_fabricated_sample(dataset_id: str):
    response = {
        "target_column_names": ["Fiscal year", "Project number", "Status,Maximum CIDA contribution (project-level)", "Branch ID",
                                "Branch name", "Division ID", "Division name", "Section ID", "Section name",
                                "Regional program (marker)", "Fund centre ID", "Fund centre name",
                                "Untied amount(Project-level budget)", "FSTC percent,IRTC percent,CFLI (marker)",
                                "CIDA business delivery model (old)", "Programming process (new)",
                                "Bilateral aid (international marker)", "PBA type", "Enviromental sustainability (marker)",
                                "Climate change adaptation (marker)", "Climate change mitigation (marker)",
                                "Desertification (marker)", "Participatory development and good governance"],
        "target_sample_data": [["200o2010", "A017716001", "Closed", "8500267.65", "B4100", "OGM Asia Pacific", "D4100", "OAD Asia Programming", "S4122", "Bangladesh Section", "0", "4122", "Bangladesh", "8500267.65", "1.0", "0.0", "0", "Directive", "Uncoded: Pre-APP", "1", "Not PBA", "0", "0", "0", "0", "0"],
                               ["20p92010", "A017716001", "Closed", "8500267.65", "B4100", "OGM Asia Pacific", "D4100", "OAD Asia Programming", "S4122", "Bangladesh Section", "0", "4122", "Bangladesh", "8500267.65", "1.0", "0.0", "0", "Directive", "Uncoded: Pre-APP", "1", "Not PBA", "0", "0", "0", "0", "0"],
                               ["2009q010", "A017716001", "Closed", "8500267.65", "B4100", "OGM Asia Pacific", "D4100", "OAD Asia Programming", "S4122", "Bangladesh Section", "0", "4122", "Bangladesh", "8500267.65", "1.0", "0.0", "0", "Directive", "Uncoded: Pre-APP", "1", "Not PBA", "0", "0", "0", "0", "0"],
                               ["20992010", "A018652001", "Closed", "500458.33", "B3100", "EGM Europe", " Middle East and Maghreb", "D3100", "EDD Europe-Middle East Programming", "S4264", "West Bank Gaza & Palestinian Refugees", "0", "4265", "West Bank Gaza", "0.0", "0.0", "0.0", "0", "Directive", "Uncoded: Pre-APP", "1", "Not PBA", "0", "0", "0", "0", "0"],
                               ["2p092010", "A018893001", "Closed", "14410076.46", "B4200", "WGM Sub-Saharan Africa", "D4207", "WWD West & Central Africa", "S4215", "Mali Program Section", "0", "4216", "Mali", "14410076.46", "1.0", "0.0", "0", "Responsive", "Uncoded: Pre-APP", "1", "Not PBA", "0", "0", "0", "0", "1"],
                               ["2009201o", "A018893001", "Closed", "14410076.46", "B4200", "WGM Sub-Saharan Africa", "D4207", "WWD West & Central Africa", "S4215", "Mali Program Section", "0", "4216", "Mali", "14410076.46", "1.0", "0.0", "0", "Responsive", "Uncoded: Pre-APP", "1", "Not PBA", "0", "0", "0", "0", "1"],
                               ["200920w0", "A018893001", "Closed", "14410076.46", "B4200", "WGM Sub-Saharan Africa", "D4207", "WWD West & Central Africa", "S4215", "Mali Program Section", "0", "4216", "Mali", "14410076.46", "1.0", "0.0", "0", "Responsive", "Uncoded: Pre-APP", "1", "Not PBA", "0", "0", "0", "0", "1"],
                               ["20092p10", "A019043001", "Closed", "13743220.56", "B4100", "OGM Asia Pacific", "D4100", "OAD Asia Programming", "S4141", "OAA Afghanistan", " Pakistan and Sri Lanka", "0", "4124", "Pakistan", "13743220.56", "1.0", "0.0", "0", "Responsive", "Uncoded: Pre-APP", "1", "Not PBA", "0", "0", "0", "0", "1"],
                               ["20092019", "A019043001", "Closed", "13743220.56", "B4100", "OGM Asia Pacific", "D4100", "OAD Asia Programming", "S4141", "OAA Afghanistan", " Pakistan and Sri Lanka", "0", "4124", "Pakistan", "13743220.56", "1.0", "0.0", "0", "Responsive", "Uncoded: Pre-APP", "1", "Not PBA", "0", "0", "0", "0", "1"],
                               ["200o2010", "A019043001", "Closed", "13743220.56", "B4100", "OGM Asia Pacific", "D4100", "OAD Asia Programming", "S4141", "OAA Afghanistan", " Pakistan and Sri Lanka", "0", "4124", "Pakistan", "13743220.56", "1.0", "0.0", "0", "Responsive", "Uncoded: Pre-APP", "1", "Not PBA", "0", "0", "0", "0", "1"]],
        "source_column_names": ["miller2_FiscalYear", "miller2_TradeDevelopment(marker)", "miller2_Biodiversity(marker)",
                                "miller2_UrbanIssues(marker)", "miller2_ChildrenIssues(marker)", "miller2_YouthIssues(marker)",
                                "miller2_IndigenousIssues(marker)", "miller2_DisabilityIssues(marker)",
                                "miller2_ICTAsAToolForDevelopment(marker)", "miller2_KnowledgeForDevelopment(marker)",
                                "miller2_GenderEquality(marker)", "miller2_OrganisationID", "miller2_OrganisationName",
                                "miller2_OrganisationType", "miller2_OrganisationClass", "miller2_OrganisationSub-class",
                                "miller2_ContinentID", "miller2_ContinentName", "miller2_ProjectBrowserCountryID",
                                "miller2_CountryRegionID", "miller2_CountryRegionName", "miller2_CountryRegionPercent",
                                "miller2_SectorID", "miller2_SectorName", "miller2_SectorPercent", "miller2_AmountSpent"],
        "source_sample_data": [["2009/2010", "0", "0", "0", "0", "0", "0", "0", "1", "1", "0", "1005967", "IAEA - International Atomic Energy Agency Technical Cooperation Fund", "Foreign Non-Profit Making", "Multilateral", "UNITED NATIONS", "3", "Asia", "X3", "X3", "Asia MC", "0.22", "33140", "Multilateral trade negotiations", "0.14", "75768.0"],
                               ["2009/2010", "0", "0", "0", "0", "0", "0", "0", "1", "1", "0", "1005967", "IAEA - International Atomic Energy Agency Technical Cooperation Fund", "Foreign Non-Profit Making", "Multilateral", "UNITED NATIONS", "3", "Asia", "X3", "X3", "Asia MC", "0.22", "99810", "Sectors not specified", "0.505", "273306.0"],
                               ["2009/2010", "0", "0", "0", "0", "0", "0", "0", "1", "1", "0", "1005967", "IAEA - International Atomic Energy Agency Technical Cooperation Fund", "Foreign Non-Profit Making", "Multilateral", "UNITED NATIONS", "5", "Europe", "X4", "X4", "Europe MC", "0.32", "12191", "Medical services", "0.268", "210969.6"],
                               ["2009/2010", "0", "0", "0", "0", "0", "0", "0", "1", "1", "0", "1005967", "IAEA - International Atomic Energy Agency Technical Cooperation Fund", "Foreign Non-Profit Making", "Multilateral", "UNITED NATIONS", "5", "Europe", "X4", "X4", "Europe MC", "0.32", "14015", "Water resources conservation (including data collection)", "0.047", "36998.4"],
                               ["2009/2010", "0", "0", "0", "0", "0", "0", "0", "1", "1", "0", "1005967", "IAEA - International Atomic Energy Agency Technical Cooperation Fund", "Foreign Non-Profit Making", "Multilateral", "UNITED NATIONS", "5", "Europe", "X4", "X4", "Europe MC", "0.32", "23510", "Nuclear energy electric power plants", "0.04", "31488.0"],
                               ["2009/2010", "0", "0", "0", "0", "0", "0", "0", "1", "1", "0", "1005967", "IAEA - International Atomic Energy Agency Technical Cooperation Fund", "Foreign Non-Profit Making", "Multilateral", "UNITED NATIONS", "5", "Europe", "X4", "X4", "Europe MC", "0.32", "33140", "Multilateral trade negotiations", "0.14", "110208.0"],
                               ["2009/2010", "0", "0", "0", "0", "0", "0", "0", "1", "1", "0", "1005967", "IAEA - International Atomic Energy Agency Technical Cooperation Fund", "Foreign Non-Profit Making", "Multilateral", "UNITED NATIONS", "5", "Europe", "X4", "X4", "Europe MC", "0.32", "99810", "Sectors not specified", "0.505", "397536.0"],
                               ["2009/2010", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "1001300", "PAHO - Pan American Health Organization", "Foreign Non-Profit Making", "Multilateral", "REGIONAL ORGANIZATION", "1", "America", "SV", "SV", "El Salvador", "1.0", "72010", "Material relief assistance and services", "1.0", "300000.0"],
                               ["2009/2010", "0", "0", "0", "0", "0", "0", "0", "0", "0", "1", "1014663", "ICRC Appeals via CRCS", "Canadian Non-Profit Making", "Civil Society", "INTERNATIONAL NGO", "2", "Africa", "X1", "X1", "Africa MC", "0.5", "72010", "Material relief assistance and services", "1.0", "1500000.0"],
                               ["2009/2010", "0", "0", "0", "0", "0", "0", "0", "0", "0", "1", "1014663", "ICRC Appeals via CRCS", "Canadian Non-Profit Making", "Civil Society", "INTERNATIONAL NGO", "1", "America", "X2", "X2", "Americas MC", "0.15", "72010", "Material relief assistance and services", "1.0", "450000.0"]],
    }
    return jsonify(response)


@app.route('/valentine/fabricate_data', methods=['POST'])
def valentine_fabricate_data():
    form = DatasetFabricationForm()
    if not form.validate_on_submit():
        abort(400, form.errors)

    tmp_dir: str = gettempdir()
    bucket_name = "FabricatedData"

    filename = secure_filename(form.resource.data.filename)
    file_path = path.join(tmp_dir, filename)  # File location
    form.resource.data.save(file_path)

    if form.fabricate_joinable.data:
        # bool array in the format noisy instances, noisy schemata, verbatim instances and verbatim schemata
        what_to_fabricate: list[bool] = form.joinable_specs.data
        pairs: int = form.joinable_pairs.data
        pass

    # example of storing data to minio
    filename = ...
    file = ...
    minio_client.fput_object(bucket_name, filename, file)

    if form.fabricate_unionable.data:
        # bool array in the format noisy instances, noisy schemata, verbatim instances and verbatim schemata
        what_to_fabricate: list[bool] = form.unionable_specs.data
        pairs: int = form.unionable_pairs.data
        pass

    if form.fabricate_view_unionable.data:
        # bool array in the format noisy instances, noisy schemata, verbatim instances and verbatim schemata
        what_to_fabricate: list[bool] = form.fabricate_view_unionable.data
        pairs: int = form.view_unionable_pairs.data
        pass

    if form.fabricate_semantically_joinable.data:
        # bool array in the format noisy instances, noisy schemata, verbatim instances and verbatim schemata
        what_to_fabricate: list[bool] = form.semantically_joinable_specs.data
        pairs: int = form.semantically_joinable_pairs.data
        pass


if __name__ != '__main__':
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)


if __name__ == '__main__':
    app.run(debug=False)
