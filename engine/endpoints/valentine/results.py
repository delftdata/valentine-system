import base64
from io import BytesIO
from time import sleep

from flask import Blueprint, jsonify, Response

from engine import VALENTINE_PLOTS_MINIO_BUCKET
from engine.db import minio_client
from engine.data_sources.minio.minio_utils import list_bucket_files

app_valentine_results = Blueprint('app_valentine_results', __name__)


@app_valentine_results.get('/valentine/results/get_fabricated_data')
def valentine_get_fabricated_data():
    # TODO IMPLEMENT ME
    # fabricated_datasets = {
    #     "miller": ["miller_both_0_1_ac1_av", "miller_both_50_70_ac4_av", "miller_both_0_1_ac1_ev",
    #                 "miller_both_50_70_ac4_ev", "miller_both_0_1_ac2_av", "miller_both_50_70_ac5_av", "miller_both_0_1_ac2_ev",
    #                 "miller_both_50_70_ac5_ev", "miller_both_0_1_ac3_av", "miller_both_50_70_ec_av", "miller_both_0_1_ac3_ev",
    #                 "miller_both_50_70_ec_ev", "miller_both_0_1_ac4_av", "miller_horizontal_0_ac1_av", "miller_both_0_1_ac4_ev",
    #                 "miller_horizontal_0_ac1_ev", "miller_both_0_1_ac5_av", "miller_horizontal_0_ac2_av", "miller_both_0_1_ac5_ev",
    #                 "miller_horizontal_0_ac2_ev", "miller_both_0_1_ec_av", "miller_horizontal_0_ac3_av", "miller_both_0_1_ec_ev",
    #                 "miller_horizontal_0_ac3_ev", "miller_both_0_30_ac1_av", "miller_horizontal_0_ac4_av", "miller_both_0_30_ac1_ev",
    #                 "miller_horizontal_0_ac4_ev", "miller_both_0_30_ac2_av", "miller_horizontal_0_ac5_av", "miller_both_0_30_ac2_ev",
    #                 "miller_horizontal_0_ac5_ev", "miller_both_0_30_ac3_av", "miller_horizontal_0_ec_av", "miller_both_0_30_ac3_ev",
    #                 "miller_horizontal_0_ec_ev", "miller_both_0_30_ac4_av", "miller_horizontal_100_ac1_av", "miller_both_0_30_ac4_ev",
    #                 "miller_horizontal_100_ac1_ev"],
    # }
    fabricated_datasets = {}
    return jsonify(fabricated_datasets)


@app_valentine_results.get('/valentine/results/delete_fabricated_dataset/<dataset_id>')
def valentine_delete_fabricated_dataset(dataset_id: str):
    # TODO IMPLEMENT ME
    sleep(0.1)
    return Response(f"Dataset {dataset_id} deleted successfully", status=200)


@app_valentine_results.get('/valentine/results/download_fabricated_dataset/<dataset_id>')
def valentine_download_fabricated_dataset(dataset_id: str):
    # TODO IS THIS NEEDED?
    obj_size = minio_client.stat_object('VaLeNtInE', 'output.zip').size
    data = BytesIO(list(minio_client.get_object('VaLeNtInE', 'output.zip').stream(obj_size))[0])
    return Response(data, headers={'Content-Type': 'application/zip',
                                   'Content-Disposition': 'attachment; filename=%s;' % dataset_id
                                   }, status=200)


@app_valentine_results.get('/valentine/results/download_boxplots/<job_id>')
def valentine_download_boxplots(job_id: str):
    folder_contents = list_bucket_files(VALENTINE_PLOTS_MINIO_BUCKET, minio_client)
    results = folder_contents[job_id]

    data = list()
    plots = results.keys()
    for plot in plots:
        obj_size = minio_client.stat_object(VALENTINE_PLOTS_MINIO_BUCKET, results[plot][0]).size
        img = BytesIO(list(minio_client.get_object(VALENTINE_PLOTS_MINIO_BUCKET, results[plot][0]).stream(obj_size))[0])
        encoded_img = base64.encodebytes(img.getvalue()).decode()
        data.append(encoded_img)

    return jsonify({'result': data})
