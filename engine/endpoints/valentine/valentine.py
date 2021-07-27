import json
import os
import uuid
from itertools import product

from flask import jsonify, abort, request, Response, Blueprint

from engine import VALENTINE_FABRICATED_MINIO_BUCKET, app, VALENTINE_RESULTS_MINIO_BUCKET, TMP_MINIO_BUCKET
from engine.db import minio_client
from engine.data_sources.minio.minio_utils import list_bucket_files
from engine.forms import DatasetFabricationForm
from engine.utils.api_utils import ValentineBenchmarkPayload, get_valentine_benchmark_payload, get_params_from_str_input
from engine.celery_tasks.valentine import create_fabricated_data, run_single_benchmark_task, generate_boxplot_celery


app_valentine = Blueprint('app_valentine', __name__)


@app_valentine.post('/valentine/submit_data_fabrication_job')
def valentine_fabricate_data():
    form = DatasetFabricationForm()
    if not form.validate_on_submit():
        abort(400, form.errors)
    job_uuid: str = str(uuid.uuid4())
    uploaded_file = form.resource.data
    uploaded_json_schema: dict = json.loads(form.json_schema.data.read())
    size = os.fstat(uploaded_file.fileno()).st_size
    minio_client.put_object(TMP_MINIO_BUCKET, uploaded_file.filename, uploaded_file, size)

    fabrication_variants = (form.fabricate_joinable.data, form.fabricate_unionable.data,
                            form.fabricate_view_unionable.data, form.fabricate_semantically_joinable.data)
    fabrication_parameters = (form.joinable_specs.data, form.unionable_specs.data,
                              form.view_unionable_specs.data, form.semantically_joinable_specs.data)
    fabrication_pairs = (form.joinable_pairs.data, form.unionable_pairs.data,
                         form.view_unionable_pairs.data, form.semantically_joinable_pairs.data)
    create_fabricated_data.s(uploaded_file.filename,
                             uploaded_json_schema,
                             form.dataset_group_name.data.replace(' ', '_'),
                             fabrication_variants,
                             fabrication_parameters,
                             fabrication_pairs).apply_async()

    return jsonify(job_uuid)


@app_valentine.post('/valentine/generate_boxplot/<job_id>')
def valentine_generate_boxplot(job_id: str):
    folder_contents = list_bucket_files(VALENTINE_RESULTS_MINIO_BUCKET, job_id, minio_client)
    generate_boxplot_celery.s(folder_contents, job_id).apply_async()
    return Response('Success', status=200)


@app_valentine.post('/valentine/submit_benchmark_job')
def valentine_submit_benchmark_job():
    payload: ValentineBenchmarkPayload = get_valentine_benchmark_payload(request.json)
    dataset_group_name = payload.dataset_name
    dataset_folders = list_bucket_files(VALENTINE_FABRICATED_MINIO_BUCKET, dataset_group_name, minio_client)
    job_uuid: str = str(uuid.uuid4())
    algorithm_configs = payload.algorithm_params
    for dataset_name in dataset_folders.keys():
        dataset_pairs = dataset_folders[dataset_name]
        for combination in product(dataset_pairs.items(), algorithm_configs):
            dataset, algorithm_config = combination
            dataset_name, dataset_paths = dataset
            algorithm_name = list(algorithm_config.keys())[0]
            algorithm_params: dict = list(algorithm_config.values())[0]
            processed_params: dict = {}
            for param_name, param_values in algorithm_params.items():
                processed_params[param_name] = get_params_from_str_input(param_values)
            for params in product(*processed_params.values()):
                single_algorithm_params: dict[str, object] = dict(zip(processed_params.keys(), params))
                app.logger.info(f" Sending benchmarking job for -> | "
                                f"Dataset: {dataset_name} | "
                                f"Algorithm: {algorithm_name} | "
                                f"Algorithm parameters: {single_algorithm_params}")
                run_single_benchmark_task.s(dataset_name, dataset_group_name, job_uuid,
                                            dataset_paths, algorithm_name, single_algorithm_params).apply_async()
    return jsonify(job_uuid)
