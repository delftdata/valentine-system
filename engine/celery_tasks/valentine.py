import os
import re
from timeit import default_timer

from engine import app, celery, TMP_MINIO_BUCKET, VALENTINE_FABRICATED_MINIO_BUCKET, \
    VALENTINE_RESULTS_MINIO_BUCKET, VALENTINE_PLOTS_MINIO_BUCKET, VALENTINE_METRICS_TO_COMPUTE
from engine.algorithms.algorithms import schema_only_algorithms
from engine.data_sources.valentine.golden_standard import GoldenStandardLoader
from engine.data_sources.valentine.utils import BenchmarkFiles
from engine.data_sources.valentine.valentine_table import ValentineTable
from engine.db import minio_client
from engine.data_sources.minio.minio_utils import get_pandas_df_from_minio_csv_file, get_dict_from_minio_json_file, \
    store_dict_to_minio_as_json
from engine.fabricator.dataset_generator import valentine_fabricator
from engine.utils.api_utils import get_matcher
from engine.utils.valentine_plots import ValentinePlots
from engine.data_sources.valentine import metrics as valentine_metric_functions


@celery.task
def create_fabricated_data(file_name: str,
                           json_schema: dict,
                           dataset_group_name: str,
                           fabrication_variants: tuple[bool, bool, bool, bool],
                           fabrication_parameters: tuple[list[bool], list[bool], list[bool], list[bool]],
                           fabrication_pairs: tuple[int, int, int, int]):

    df = get_pandas_df_from_minio_csv_file(minio_client, TMP_MINIO_BUCKET, file_name, 1, None)  # the loaded csv file

    fbr_joinable, fbr_unionable, fbr_view_unionable, fbr_semantically_joinable = fabrication_variants
    joinable_specs, unionable_specs, view_unionable_specs, semantically_joinable_specs = fabrication_parameters
    joinable_pairs, unionable_pairs, view_unionable_pairs, semantically_joinable_pairs = fabrication_pairs

    if not minio_client.bucket_exists(VALENTINE_FABRICATED_MINIO_BUCKET):
        minio_client.make_bucket(VALENTINE_FABRICATED_MINIO_BUCKET)

    if fbr_joinable:
        app.logger.info(f"Fabricating Joinable data for: {file_name}")
        # bool array in the format noisy instances, noisy schemata, verbatim instances and verbatim schemata
        what_to_fabricate: list[bool] = joinable_specs
        pairs: int = joinable_pairs

        valentine_fabricator('Joinable', what_to_fabricate, pairs, df, json_schema, dataset_group_name,
                             file_name.split('.')[0], minio_client, VALENTINE_FABRICATED_MINIO_BUCKET)

    if fbr_unionable:
        app.logger.info(f"Fabricating Unionable data for: {file_name}")
        # bool array in the format noisy instances, noisy schemata, verbatim instances and verbatim schemata
        what_to_fabricate: list[bool] = unionable_specs
        pairs: int = unionable_pairs

        valentine_fabricator('Unionable', what_to_fabricate, pairs, df, json_schema, dataset_group_name,
                             file_name.split('.')[0], minio_client, VALENTINE_FABRICATED_MINIO_BUCKET)

    if fbr_view_unionable:
        app.logger.info(f"Fabricating View Unionable data for: {file_name}")
        # bool array in the format noisy instances, noisy schemata, verbatim instances and verbatim schemata
        what_to_fabricate: list[bool] = view_unionable_specs
        pairs: int = view_unionable_pairs

        valentine_fabricator('View-Unionablle', what_to_fabricate, pairs, df, json_schema, dataset_group_name,
                             file_name.split('.')[0], minio_client, VALENTINE_FABRICATED_MINIO_BUCKET)

    if fbr_semantically_joinable:
        app.logger.info(f"Fabricating Semantically Joinable data for: {file_name}")
        # bool array in the format noisy instances, noisy schemata, verbatim instances and verbatim schemata
        what_to_fabricate: list[bool] = semantically_joinable_specs
        pairs: int = semantically_joinable_pairs

        valentine_fabricator('Semantically-Joinable', what_to_fabricate, pairs, df, json_schema, dataset_group_name,
                             file_name.split('.')[0], minio_client, VALENTINE_FABRICATED_MINIO_BUCKET)

    minio_client.remove_object(TMP_MINIO_BUCKET, file_name)


@celery.task
def run_single_benchmark_task(dataset_name: str,
                              dataset_group_name: str,
                              job_uuid: str,
                              dataset_paths: list[str],
                              matching_algorithm: str,
                              algorithm_params: dict[str, object]):

    if matching_algorithm == 'EmbDI':   # EmbDI is not integrated to the system yet
        return

    matcher = get_matcher(matching_algorithm, algorithm_params)

    file_paths = BenchmarkFiles(dataset_paths)

    load = False if matching_algorithm in schema_only_algorithms else True

    time_start_load = default_timer()

    source_table: ValentineTable = ValentineTable(minio_client, file_paths.source_data, file_paths.source_schema,
                                                  dataset_name + '_source', dataset_group_name, load_instances=load)
    target_table: ValentineTable = ValentineTable(minio_client, file_paths.target_data, file_paths.target_schema,
                                                  dataset_name + '_target', dataset_group_name, load_instances=load)

    time_start_algorithm = default_timer()

    matches = matcher.get_matches(source_table, target_table)

    time_end = default_timer()

    run_times = {"total_time": time_end - time_start_load, "algorithm_time": time_end - time_start_algorithm}

    golden_standard = GoldenStandardLoader(get_dict_from_minio_json_file(minio_client,
                                                                         VALENTINE_FABRICATED_MINIO_BUCKET,
                                                                         file_paths.golden_standard_path))

    metric_fns = [getattr(valentine_metric_functions, met) for met in VALENTINE_METRICS_TO_COMPUTE['names']]
    final_metrics = dict()

    valentine_matches = {((match['source']['tbl_nm'], match['source']['clm_nm']),
                          (match['target']['tbl_nm'], match['target']['clm_nm'])): match['sim'] for match in matches}

    for metric in metric_fns:
        if metric.__name__ != "precision_at_n_percent":
            if metric.__name__ in ['precision', 'recall', 'f1_score'] and matching_algorithm != "Coma":
                # Do not use the 1-1 match filter on Coma
                final_metrics[metric.__name__] = metric(valentine_matches, golden_standard, True)
            else:
                final_metrics[metric.__name__] = metric(valentine_matches, golden_standard)
        else:
            for n in VALENTINE_METRICS_TO_COMPUTE['args']['n']:
                final_metrics[metric.__name__.replace('_n_', '_' + str(n) + '_')] = metric(valentine_matches,
                                                                                           golden_standard, n)
    name = f'{dataset_name}__{matching_algorithm}{algorithm_params}'
    valentine_matches = {str(k): v for k, v in valentine_matches.items()}
    output = {"name": name, "matches": valentine_matches, "metrics": final_metrics, "run_times": run_times}
    safe_dataset_name = re.sub('\\W+', '_', str(name))
    file_path = f"{job_uuid}/{matching_algorithm}/{safe_dataset_name}.json"
    store_dict_to_minio_as_json(minio_client, output, VALENTINE_RESULTS_MINIO_BUCKET, file_path)


@celery.task
def generate_boxplot_celery(results: dict, job_id: str):
    plots = ValentinePlots()

    for algorithm_name, result_paths in results.items():
        for result_path in result_paths:
            split_path = result_path.split(os.path.sep)
            filename = split_path[len(split_path) - 1].split('.')[:-1][0]

            # This contains a single json file information
            evaluation_result: dict = get_dict_from_minio_json_file(minio_client,
                                                                    VALENTINE_RESULTS_MINIO_BUCKET, result_path)
            plots.total_metrics[filename] = evaluation_result['metrics']
            plots.run_times[filename] = evaluation_result['run_times']['total_time']

    plots.read_data()
    instance, schema, hybrid = plots.create_box_plot()

    instance.seek(0)
    schema.seek(0)
    hybrid.seek(0)

    minio_client.put_object(VALENTINE_PLOTS_MINIO_BUCKET, f"{job_id}/instance.png", instance, len(instance.getvalue()))
    minio_client.put_object(VALENTINE_PLOTS_MINIO_BUCKET, f"{job_id}/schema.png", schema, len(schema.getvalue()))
    minio_client.put_object(VALENTINE_PLOTS_MINIO_BUCKET, f"{job_id}/hybrid.png", hybrid, len(hybrid.getvalue()))
