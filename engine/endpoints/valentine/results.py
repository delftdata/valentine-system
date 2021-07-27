import base64
import json
import os
from io import BytesIO
from flask import Blueprint, jsonify, Response, abort
from minio.deleteobjects import DeleteObject

from engine import VALENTINE_PLOTS_MINIO_BUCKET, VALENTINE_FABRICATED_MINIO_BUCKET, VALENTINE_RESULTS_MINIO_BUCKET
from engine.db import minio_client
from engine.data_sources.minio.minio_utils import list_bucket_files, get_column_sample_from_minio_csv_file2, \
    download_zipped_data_from_minio, get_dict_from_minio_json_file

app_valentine_results = Blueprint('app_valentine_results', __name__)


@app_valentine_results.get('/valentine/results/download_fabricated_dataset/<dataset_group_name>')
def valentine_download_fabricated_dataset(dataset_group_name: str):
    fabricated_dataset_pairs = minio_client.list_objects(VALENTINE_FABRICATED_MINIO_BUCKET,
                                                         prefix=dataset_group_name + os.path.sep,
                                                         recursive=True)
    zip_path = download_zipped_data_from_minio(minio_client, VALENTINE_FABRICATED_MINIO_BUCKET,
                                               fabricated_dataset_pairs, dataset_group_name)
    return Response(open(zip_path, 'rb'),
                    headers={'Content-Type': 'application/zip',
                             'Content-Disposition': 'attachment; filename=%s;' % dataset_group_name},
                    status=200)


@app_valentine_results.get('/valentine/results/get_fabricated_dataset_groups')
def valentine_get_fabricated_dataset_groups():
    fabricated_data = [x.object_name[:-1] for x in
                       minio_client.list_objects(VALENTINE_FABRICATED_MINIO_BUCKET)]
    return jsonify(fabricated_data)


@app_valentine_results.get('/valentine/results/get_fabricated_dataset_group_pairs/<dataset_group_name>')
def valentine_get_fabricated_dataset_group_pairs(dataset_group_name: str):
    fabricated_dataset_pairs = minio_client.list_objects(VALENTINE_FABRICATED_MINIO_BUCKET,
                                                         prefix=dataset_group_name + os.path.sep,
                                                         recursive=True)
    fabricated_dataset_pair_names = [pair.object_name.split(os.path.sep)[-2] for pair in fabricated_dataset_pairs]
    return jsonify(sorted(list(set(fabricated_dataset_pair_names))))


@app_valentine_results.get('/valentine/results/get_fabricated_pair_sample/<dataset_group_name>/<pair_name>')
def valentine_get_fabricated_pair_sample(dataset_group_name: str, pair_name: str):
    fabricated_data = [x.object_name for x in
                       minio_client.list_objects(VALENTINE_FABRICATED_MINIO_BUCKET,
                                                 prefix=dataset_group_name + os.path.sep,
                                                 recursive=True)
                       if pair_name in x.object_name and x.object_name.endswith('.csv')]

    target_dataset = [], []
    source_dataset = [], []

    for dataset in fabricated_data:
        if dataset.endswith('source.csv'):
            source_dataset = get_column_sample_from_minio_csv_file2(minio_client,
                                                                    VALENTINE_FABRICATED_MINIO_BUCKET,
                                                                    dataset,
                                                                    10)
        else:
            target_dataset = get_column_sample_from_minio_csv_file2(minio_client,
                                                                    VALENTINE_FABRICATED_MINIO_BUCKET,
                                                                    dataset,
                                                                    10)

    response = {
        "target_column_names": target_dataset[0],
        "target_sample_data": target_dataset[1],
        "source_column_names": source_dataset[0],
        "source_sample_data":  source_dataset[1]
    }

    return jsonify(response)


@app_valentine_results.delete('/valentine/results/delete_fabricated_dataset_pair/<dataset_group_name>/<pair_name>')
def valentine_delete_fabricated_dataset_pair(dataset_group_name: str, pair_name: str):
    delete_object_list = [DeleteObject(x.object_name) for x in
                          minio_client.list_objects(VALENTINE_FABRICATED_MINIO_BUCKET,
                                                    prefix=dataset_group_name + os.path.sep,
                                                    recursive=True) if pair_name in x.object_name]
    errors = minio_client.remove_objects(VALENTINE_FABRICATED_MINIO_BUCKET, delete_object_list)
    if len(list(errors)) > 0:
        abort(400, "An error occurred when deleting objects from minio ")
    return Response(f"Pair {pair_name} deleted successfully", status=200)


@app_valentine_results.get('/valentine/results/download_fabricated_dataset_pair/<dataset_group_name>/<pair_name>')
def valentine_download_fabricated_dataset_pair(dataset_group_name: str, pair_name: str):
    fabricated_dataset_pairs = [x for x in minio_client.list_objects(VALENTINE_FABRICATED_MINIO_BUCKET,
                                                                     prefix=dataset_group_name + os.path.sep,
                                                                     recursive=True)
                                if pair_name in x.object_name]
    zip_path = download_zipped_data_from_minio(minio_client, VALENTINE_FABRICATED_MINIO_BUCKET,
                                               fabricated_dataset_pairs, dataset_group_name)
    return Response(open(zip_path, 'rb'),
                    headers={'Content-Type': 'application/zip',
                             'Content-Disposition': 'attachment; filename=%s;' % dataset_group_name},
                    status=200)


@app_valentine_results.get('/valentine/results/get_evaluation_results')
def valentine_get_evaluation_results():
    folder_contents = list_bucket_files(VALENTINE_RESULTS_MINIO_BUCKET, '', minio_client)
    evaluation_results = []
    for job in folder_contents:
        job_id, dataset_group = job.split('__dataset_group__')
        evaluation_results.append({'job_id': job_id, 'dataset_group': dataset_group.split(os.path.sep)[0]})
    return jsonify(evaluation_results)


@app_valentine_results.get('/valentine/results/get_evaluation_dataset_pairs/<job_id>/<dataset_group>')
def valentine_get_evaluation_dataset_pairs(job_id: str, dataset_group: str):
    folder_contents = list_bucket_files(VALENTINE_RESULTS_MINIO_BUCKET,
                                        f'{job_id}__dataset_group__{dataset_group}',
                                        minio_client)
    evaluation_result_pairs = []
    for job, dataset_pairs in folder_contents.items():
        _, algorithm = job.split(os.path.sep)
        for dataset_pair in dataset_pairs.keys():
            evaluation_result_pairs.append({'algorithm': algorithm, 'pair_name': dataset_pair})
    return jsonify(evaluation_result_pairs)


@app_valentine_results.get('/valentine/results/get_evaluation_dataset_pair_spurious_results/'
                           '<job_id>/<dataset_group>/<algorithm>/<dataset_pair>')
def valentine_get_evaluation_dataset_pair_spurious_results(job_id: str, dataset_group: str,
                                                           algorithm: str, dataset_pair: str):
    object_path = f'{job_id}__dataset_group__{dataset_group}/{algorithm}/{dataset_pair}'

    spurious_results = get_dict_from_minio_json_file(minio_client,
                                                     VALENTINE_RESULTS_MINIO_BUCKET,
                                                     object_path)["metrics"]["get_spurious_results_at_sizeof_ground_truth"]
    result = []
    for spurious_result in spurious_results:
        result.append([spurious_result['Column 1'], spurious_result['Column 2'],
                       spurious_result['Similarity'], spurious_result['Type']])
    return jsonify(result)


@app_valentine_results.get('/valentine/results/download_evaluation_results/<job_id>/<dataset_group>')
def valentine_download_evaluation_results(job_id: str, dataset_group: str):
    fabricated_dataset_pairs = minio_client.list_objects(VALENTINE_RESULTS_MINIO_BUCKET,
                                                         prefix=f'{job_id}__dataset_group__{dataset_group}/',
                                                         recursive=True)
    zip_path = download_zipped_data_from_minio(minio_client, VALENTINE_RESULTS_MINIO_BUCKET,
                                               fabricated_dataset_pairs, job_id)
    return Response(open(zip_path, 'rb'),
                    headers={'Content-Type': 'application/zip',
                             'Content-Disposition': 'attachment; filename=%s;' % job_id},
                    status=200)


@app_valentine_results.get('/valentine/results/download_pairs_evaluation_result/'
                           '<job_id>/<dataset_group>/<algorithm>/<dataset_pair>')
def valentine_download_pairs_evaluation_result(job_id: str, dataset_group: str,
                                               algorithm: str, dataset_pair: str):
    object_path = f'{job_id}__dataset_group__{dataset_group}/{algorithm}/{dataset_pair}'
    result = json.dumps(get_dict_from_minio_json_file(minio_client,
                                                      VALENTINE_RESULTS_MINIO_BUCKET,
                                                      object_path), indent=2)
    return Response(result,
                    headers={'Content-Type': 'application/json',
                             'Content-Disposition': 'attachment; filename=%s;' % dataset_pair},
                    status=200)


@app_valentine_results.get('/valentine/results/download_boxplots/<job_id>')
def valentine_download_boxplots(job_id: str):
    folder_contents = list_bucket_files(VALENTINE_PLOTS_MINIO_BUCKET, '', minio_client)
    results = folder_contents[job_id]

    data = list()
    plots = results.keys()
    for plot in plots:
        obj_size = minio_client.stat_object(VALENTINE_PLOTS_MINIO_BUCKET, results[plot][0]).size
        img = BytesIO(list(minio_client.get_object(VALENTINE_PLOTS_MINIO_BUCKET, results[plot][0]).stream(obj_size))[0])
        encoded_img = base64.encodebytes(img.getvalue()).decode()
        data.append(encoded_img)

    return jsonify({'result': data})
