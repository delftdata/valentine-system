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


@app_valentine.route('/valentine/upload_dataset', methods=['POST'])
def valentine_upload_dataset():
    # TODO: IS THIS NEEDED?
    try:
        uploaded_file = request.files["file"]
        if uploaded_file:
            size = os.fstat(uploaded_file.fileno()).st_size
            minio_client.put_object('VaLeNtInE', uploaded_file.filename, uploaded_file, size)
    except Exception:
        abort(400, "File Could not be uploaded")
    else:
        return Response("object stored in bucket", status=200)


@app_valentine.route('/valentine/get_fabricated_sample/<dataset_id>', methods=['GET'])
def valentine_get_fabricated_sample(dataset_id: str):
    # response = {
    #     "target_column_names": ["Fiscal year", "Project number", "Status,Maximum CIDA contribution (project-level)", "Branch ID",
    #                             "Branch name", "Division ID", "Division name", "Section ID", "Section name",
    #                             "Regional program (marker)", "Fund centre ID", "Fund centre name",
    #                             "Untied amount(Project-level budget)", "FSTC percent,IRTC percent,CFLI (marker)",
    #                             "CIDA business delivery model (old)", "Programming process (new)",
    #                             "Bilateral aid (international marker)", "PBA type", "Enviromental sustainability (marker)",
    #                             "Climate change adaptation (marker)", "Climate change mitigation (marker)",
    #                             "Desertification (marker)", "Participatory development and good governance"],
    #     "target_sample_data": [["200o2010", "A017716001", "Closed", "8500267.65", "B4100", "OGM Asia Pacific", "D4100", "OAD Asia Programming", "S4122", "Bangladesh Section", "0", "4122", "Bangladesh", "8500267.65", "1.0", "0.0", "0", "Directive", "Uncoded: Pre-APP", "1", "Not PBA", "0", "0", "0", "0", "0"],
    #                            ["20p92010", "A017716001", "Closed", "8500267.65", "B4100", "OGM Asia Pacific", "D4100", "OAD Asia Programming", "S4122", "Bangladesh Section", "0", "4122", "Bangladesh", "8500267.65", "1.0", "0.0", "0", "Directive", "Uncoded: Pre-APP", "1", "Not PBA", "0", "0", "0", "0", "0"],
    #                            ["2009q010", "A017716001", "Closed", "8500267.65", "B4100", "OGM Asia Pacific", "D4100", "OAD Asia Programming", "S4122", "Bangladesh Section", "0", "4122", "Bangladesh", "8500267.65", "1.0", "0.0", "0", "Directive", "Uncoded: Pre-APP", "1", "Not PBA", "0", "0", "0", "0", "0"],
    #                            ["20992010", "A018652001", "Closed", "500458.33", "B3100", "EGM Europe", " Middle East and Maghreb", "D3100", "EDD Europe-Middle East Programming", "S4264", "West Bank Gaza & Palestinian Refugees", "0", "4265", "West Bank Gaza", "0.0", "0.0", "0.0", "0", "Directive", "Uncoded: Pre-APP", "1", "Not PBA", "0", "0", "0", "0", "0"],
    #                            ["2p092010", "A018893001", "Closed", "14410076.46", "B4200", "WGM Sub-Saharan Africa", "D4207", "WWD West & Central Africa", "S4215", "Mali Program Section", "0", "4216", "Mali", "14410076.46", "1.0", "0.0", "0", "Responsive", "Uncoded: Pre-APP", "1", "Not PBA", "0", "0", "0", "0", "1"],
    #                            ["2009201o", "A018893001", "Closed", "14410076.46", "B4200", "WGM Sub-Saharan Africa", "D4207", "WWD West & Central Africa", "S4215", "Mali Program Section", "0", "4216", "Mali", "14410076.46", "1.0", "0.0", "0", "Responsive", "Uncoded: Pre-APP", "1", "Not PBA", "0", "0", "0", "0", "1"],
    #                            ["200920w0", "A018893001", "Closed", "14410076.46", "B4200", "WGM Sub-Saharan Africa", "D4207", "WWD West & Central Africa", "S4215", "Mali Program Section", "0", "4216", "Mali", "14410076.46", "1.0", "0.0", "0", "Responsive", "Uncoded: Pre-APP", "1", "Not PBA", "0", "0", "0", "0", "1"],
    #                            ["20092p10", "A019043001", "Closed", "13743220.56", "B4100", "OGM Asia Pacific", "D4100", "OAD Asia Programming", "S4141", "OAA Afghanistan", " Pakistan and Sri Lanka", "0", "4124", "Pakistan", "13743220.56", "1.0", "0.0", "0", "Responsive", "Uncoded: Pre-APP", "1", "Not PBA", "0", "0", "0", "0", "1"],
    #                            ["20092019", "A019043001", "Closed", "13743220.56", "B4100", "OGM Asia Pacific", "D4100", "OAD Asia Programming", "S4141", "OAA Afghanistan", " Pakistan and Sri Lanka", "0", "4124", "Pakistan", "13743220.56", "1.0", "0.0", "0", "Responsive", "Uncoded: Pre-APP", "1", "Not PBA", "0", "0", "0", "0", "1"],
    #                            ["200o2010", "A019043001", "Closed", "13743220.56", "B4100", "OGM Asia Pacific", "D4100", "OAD Asia Programming", "S4141", "OAA Afghanistan", " Pakistan and Sri Lanka", "0", "4124", "Pakistan", "13743220.56", "1.0", "0.0", "0", "Responsive", "Uncoded: Pre-APP", "1", "Not PBA", "0", "0", "0", "0", "1"]],
    #     "source_column_names": ["miller2_FiscalYear", "miller2_TradeDevelopment(marker)", "miller2_Biodiversity(marker)",
    #                             "miller2_UrbanIssues(marker)", "miller2_ChildrenIssues(marker)", "miller2_YouthIssues(marker)",
    #                             "miller2_IndigenousIssues(marker)", "miller2_DisabilityIssues(marker)",
    #                             "miller2_ICTAsAToolForDevelopment(marker)", "miller2_KnowledgeForDevelopment(marker)",
    #                             "miller2_GenderEquality(marker)", "miller2_OrganisationID", "miller2_OrganisationName",
    #                             "miller2_OrganisationType", "miller2_OrganisationClass", "miller2_OrganisationSub-class",
    #                             "miller2_ContinentID", "miller2_ContinentName", "miller2_ProjectBrowserCountryID",
    #                             "miller2_CountryRegionID", "miller2_CountryRegionName", "miller2_CountryRegionPercent",
    #                             "miller2_SectorID", "miller2_SectorName", "miller2_SectorPercent", "miller2_AmountSpent"],
    #     "source_sample_data": [["2009/2010", "0", "0", "0", "0", "0", "0", "0", "1", "1", "0", "1005967", "IAEA - International Atomic Energy Agency Technical Cooperation Fund", "Foreign Non-Profit Making", "Multilateral", "UNITED NATIONS", "3", "Asia", "X3", "X3", "Asia MC", "0.22", "33140", "Multilateral trade negotiations", "0.14", "75768.0"],
    #                            ["2009/2010", "0", "0", "0", "0", "0", "0", "0", "1", "1", "0", "1005967", "IAEA - International Atomic Energy Agency Technical Cooperation Fund", "Foreign Non-Profit Making", "Multilateral", "UNITED NATIONS", "3", "Asia", "X3", "X3", "Asia MC", "0.22", "99810", "Sectors not specified", "0.505", "273306.0"],
    #                            ["2009/2010", "0", "0", "0", "0", "0", "0", "0", "1", "1", "0", "1005967", "IAEA - International Atomic Energy Agency Technical Cooperation Fund", "Foreign Non-Profit Making", "Multilateral", "UNITED NATIONS", "5", "Europe", "X4", "X4", "Europe MC", "0.32", "12191", "Medical services", "0.268", "210969.6"],
    #                            ["2009/2010", "0", "0", "0", "0", "0", "0", "0", "1", "1", "0", "1005967", "IAEA - International Atomic Energy Agency Technical Cooperation Fund", "Foreign Non-Profit Making", "Multilateral", "UNITED NATIONS", "5", "Europe", "X4", "X4", "Europe MC", "0.32", "14015", "Water resources conservation (including data collection)", "0.047", "36998.4"],
    #                            ["2009/2010", "0", "0", "0", "0", "0", "0", "0", "1", "1", "0", "1005967", "IAEA - International Atomic Energy Agency Technical Cooperation Fund", "Foreign Non-Profit Making", "Multilateral", "UNITED NATIONS", "5", "Europe", "X4", "X4", "Europe MC", "0.32", "23510", "Nuclear energy electric power plants", "0.04", "31488.0"],
    #                            ["2009/2010", "0", "0", "0", "0", "0", "0", "0", "1", "1", "0", "1005967", "IAEA - International Atomic Energy Agency Technical Cooperation Fund", "Foreign Non-Profit Making", "Multilateral", "UNITED NATIONS", "5", "Europe", "X4", "X4", "Europe MC", "0.32", "33140", "Multilateral trade negotiations", "0.14", "110208.0"],
    #                            ["2009/2010", "0", "0", "0", "0", "0", "0", "0", "1", "1", "0", "1005967", "IAEA - International Atomic Energy Agency Technical Cooperation Fund", "Foreign Non-Profit Making", "Multilateral", "UNITED NATIONS", "5", "Europe", "X4", "X4", "Europe MC", "0.32", "99810", "Sectors not specified", "0.505", "397536.0"],
    #                            ["2009/2010", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "1001300", "PAHO - Pan American Health Organization", "Foreign Non-Profit Making", "Multilateral", "REGIONAL ORGANIZATION", "1", "America", "SV", "SV", "El Salvador", "1.0", "72010", "Material relief assistance and services", "1.0", "300000.0"],
    #                            ["2009/2010", "0", "0", "0", "0", "0", "0", "0", "0", "0", "1", "1014663", "ICRC Appeals via CRCS", "Canadian Non-Profit Making", "Civil Society", "INTERNATIONAL NGO", "2", "Africa", "X1", "X1", "Africa MC", "0.5", "72010", "Material relief assistance and services", "1.0", "1500000.0"],
    #                            ["2009/2010", "0", "0", "0", "0", "0", "0", "0", "0", "0", "1", "1014663", "ICRC Appeals via CRCS", "Canadian Non-Profit Making", "Civil Society", "INTERNATIONAL NGO", "1", "America", "X2", "X2", "Americas MC", "0.15", "72010", "Material relief assistance and services", "1.0", "450000.0"]],
    # }
    response = {}
    return jsonify(response)


@app_valentine.route('/valentine/submit_data_fabrication_job', methods=['POST'])
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
    create_fabricated_data.s(uploaded_file.filename, uploaded_json_schema, form.dataset_group_name.data,
                             fabrication_variants, fabrication_parameters, fabrication_pairs).apply_async()

    return jsonify(job_uuid)


@app_valentine.post('/valentine/generate_boxplot/<job_id>')
def valentine_generate_boxplot(job_id: str):
    folder_contents = list_bucket_files(VALENTINE_RESULTS_MINIO_BUCKET, minio_client)
    results = folder_contents[job_id]
    generate_boxplot_celery.s(results).apply_async()
    return Response('Success', status=200)


@app_valentine.post('/valentine/submit_benchmark_job')
def valentine_submit_benchmark_job():
    payload: ValentineBenchmarkPayload = get_valentine_benchmark_payload(request.json)
    dataset_group_name = payload.dataset_name
    bucket_folder_contents = list_bucket_files(VALENTINE_FABRICATED_MINIO_BUCKET, minio_client)
    if dataset_group_name not in bucket_folder_contents:
        abort(400, 'Dataset group does not exist')
    job_uuid: str = str(uuid.uuid4())
    dataset_folders = bucket_folder_contents[dataset_group_name]
    algorithm_configs = payload.algorithm_params
    for combination in product(dataset_folders.items(), algorithm_configs):
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


@app_valentine.get('/valentine/get_fabricated_datasets')
def valentine_get_fabricated_datasets():
    fabricated_data = [x.object_name[:-1] for x in minio_client.list_objects(VALENTINE_FABRICATED_MINIO_BUCKET)]
    return jsonify(fabricated_data)
