from itertools import product

datasets = {'miller2_both_0_1_ac1_av': ['miller2/miller2_both_0_1_ac1_av/config_miller2_both_0_1_ac1_av.ini', 'miller2/miller2_both_0_1_ac1_av/miller2_both_0_1_ac1_av_mapping.json', 'miller2/miller2_both_0_1_ac1_av/miller2_both_0_1_ac1_av_source.csv', 'miller2/miller2_both_0_1_ac1_av/miller2_both_0_1_ac1_av_source.json', 'miller2/miller2_both_0_1_ac1_av/miller2_both_0_1_ac1_av_target.csv', 'miller2/miller2_both_0_1_ac1_av/miller2_both_0_1_ac1_av_target.json'],
            'miller2_both_0_1_ac1_ev': ['miller2/miller2_both_0_1_ac1_ev/config_miller2_both_0_1_ac1_ev.ini', 'miller2/miller2_both_0_1_ac1_ev/miller2_both_0_1_ac1_ev_mapping.json', 'miller2/miller2_both_0_1_ac1_ev/miller2_both_0_1_ac1_ev_source.csv', 'miller2/miller2_both_0_1_ac1_ev/miller2_both_0_1_ac1_ev_source.json', 'miller2/miller2_both_0_1_ac1_ev/miller2_both_0_1_ac1_ev_target.csv', 'miller2/miller2_both_0_1_ac1_ev/miller2_both_0_1_ac1_ev_target.json'],
            'miller2_both_0_1_ac2_av': ['miller2/miller2_both_0_1_ac2_av/config_miller2_both_0_1_ac2_av.ini', 'miller2/miller2_both_0_1_ac2_av/miller2_both_0_1_ac2_av_mapping.json', 'miller2/miller2_both_0_1_ac2_av/miller2_both_0_1_ac2_av_source.csv', 'miller2/miller2_both_0_1_ac2_av/miller2_both_0_1_ac2_av_source.json', 'miller2/miller2_both_0_1_ac2_av/miller2_both_0_1_ac2_av_target.csv', 'miller2/miller2_both_0_1_ac2_av/miller2_both_0_1_ac2_av_target.json']}


algorithms = [{"Coma": {'max_n': 0, 'strategy': 'COMA_OPT'}},
              {"Cupid": {'leaf_w_struct': 0.2, 'w_struct': 0.2, 'th_accept': 0.7, 'th_high': 0.6,
                         'th_low': 0.35, 'th_ns': 0.7}}]


for combination in product(datasets.items(), algorithms):
    dataset, algorithm_config = combination
    dataset_name, dataset_paths = dataset
    # print(algorithm_config)
    # print(dataset_name, dataset_paths)
    # print('-------------------------')


p = ['miller2/miller2_both_0_1_ac1_av/config_miller2_both_0_1_ac1_av.ini', 'miller2/miller2_both_0_1_ac1_av/miller2_both_0_1_ac1_av_mapping.json', 'miller2/miller2_both_0_1_ac1_av/miller2_both_0_1_ac1_av_source.csv', 'miller2/miller2_both_0_1_ac1_av/miller2_both_0_1_ac1_av_source.json', 'miller2/miller2_both_0_1_ac1_av/miller2_both_0_1_ac1_av_target.csv', 'miller2/miller2_both_0_1_ac1_av/miller2_both_0_1_ac1_av_target.json']


class BenchmarkFiles:

    def __init__(self, paths: list[str]):
        for file_path in paths:
            if file_path.endswith('mapping.json'):
                self.golden_standard_path = file_path
            elif file_path.endswith('source.json'):
                self.source_schema = file_path
            elif file_path.endswith('source.csv'):
                self.source_data = file_path
            elif file_path.endswith('target.json'):
                self.target_schema = file_path
            elif file_path.endswith('target.csv'):
                self.target_data = file_path
