import copy
import json
import operator
import pandas as pd
import numpy as np
import seaborn as sns


from engine.utils.plot_utils import plot_by_data_type

algorithms = {
    'Cupid': None,
    'DistributionBased': None,
    'SimilarityFlooding': None,
    'SemProp': None,
    'JaccardLevenMatcher': None,
    'COMA-S': None,
    'COMA-SI': None,
    'EmbDI': None
}

problem_dictionary = {
    'Unionable': ['horizontal', 'unionable'],
    'View-Unionable': ['both_0_', 'viewunion'],
    'Joinable': ['both_50_', 'vertical', '_joinable'],
    'Semantically-Joinable': ['both_50_', 'vertical', '_semjoinable']
}

problems = ['Unionable', 'View-Unionable', 'Joinable', 'Semantically-Joinable']

instance_order = ['DistributionBased', 'JaccardLevenMatcher', 'COMA-SI']
schema_order = ['Cupid', 'SimilarityFlooding', 'COMA-S']
hybrid_order = ['EmbDI', 'SemProp']

current_palette = sns.color_palette('colorblind')

schema_colors = [current_palette[4], current_palette[8]]
instance_colors = [current_palette[6], current_palette[9]]
hybrid_colors = [current_palette[6], current_palette[9], '#d8f545', '#d05ef2']

map_hybrid = {
    current_palette[6]: '\\',
    current_palette[9]: 'o'
}

map_instance = {
    current_palette[6]: '/',
    current_palette[9]: 'o'
}

map_schema = {
    current_palette[4]: '\\',
    current_palette[8]: '.'
}

hatches_sch = ['\\', '.']
hatches_inst = ['/', 'o']
hatches_hybrid = ['\\', 'o']

hue_order_hybrid = ['Noisy Instances/Schemata', 'Verbatim Instances/Schemata']
hue_order_inst = ['Noisy Instances', 'Verbatim Instances']
hue_order_sch = ['Noisy Schemata']


def find_best_config_dict(source_dict: dict, target_dict: dict, to_table: dict):
    for key in source_dict.keys():
        for algo in source_dict[key].keys():
            max_item = max(source_dict[key][algo].items(), key=operator.itemgetter(1))
            target_dict[key][algo] = max_item[0]
            to_table[key][algo] = max_item[1]


def get_best_metric(source_dict: dict, target_dict: dict, index):
    for key in source_dict.keys():
        for algo in source_dict[key].keys():
            target_dict[key][algo] = source_dict[key][algo][index[key][algo]]


def determine_collumn_names_type(variables):
    if "noisy_schemata" in variables:
        return "ac"
    elif "verbatim_schemata" in variables:
        return "ec"


def determine_values_type(variables):
    if "noisy_instances" in variables:
        return "av"
    elif "verbatim_instances" in variables:
        return "ev"
    elif "semantically_joinable" in variables:
        return "av"
    else:
        return "ev"


def parse_datasets(best_prec_pd):
    category = []
    mother_table = []
    way = []
    column_names = []
    typeOfValues = []
    horizontal_overlap = []
    vertical_overlap = []

    for index, dataset in best_prec_pd.loc[:, ['Dataset']].iterrows():
        table_name = "unknown"
        for problem in ["joinable", "semantically_joinable", "unionable", "view_unionable"]:
            if problem in dataset['Dataset']:
                if problem == "joinable" and "semantically_joinable" not in dataset['Dataset']:
                    category.append("Joinable")
                    table_name = dataset['Dataset'].split('_joinable_')[0]
                elif problem == "semantically_joinable":
                    category.append("Semantically-Joinable")
                    table_name = dataset['Dataset'].split('_semantically_joinable_')[0]
                elif problem == "unionable" and "view_unionable" not in dataset['Dataset']:
                    category.append("Unionable")
                    table_name = dataset['Dataset'].split('_unionable_')[0]
                elif problem == "view_unionable":
                    category.append("View-Unionable")
                    table_name = dataset['Dataset'].split('_view_unionable_')[0]

        variables = dataset['Dataset'].split('_')
        # app.logger.info(f"{variables}")
        mother_table.append(table_name)
        if 'both' in variables:
            way.append("both")
            horizontal_overlap.append("random")
            vertical_overlap.append("random")
            column_names.append(determine_collumn_names_type(dataset['Dataset']))
            typeOfValues.append(determine_values_type(dataset['Dataset']))
        elif 'horizontal' in variables:
            way.append("horizontal")
            horizontal_overlap.append("random")
            vertical_overlap.append(None)
            column_names.append(determine_collumn_names_type(dataset['Dataset']))
            typeOfValues.append(determine_values_type(dataset['Dataset']))
        elif 'vertical' in variables:
            way.append("vertical")
            horizontal_overlap.append(None)
            vertical_overlap.append("random")
            column_names.append(determine_collumn_names_type(dataset['Dataset']))
            typeOfValues.append(determine_values_type(dataset['Dataset']))
        elif 'unionable' in variables:
            way.append(None)
            horizontal_overlap.append(None)
            vertical_overlap.append(None)
            column_names.append(determine_collumn_names_type(dataset['Dataset']))
            typeOfValues.append(determine_values_type(dataset['Dataset']))
        else:
            way.append(None)
            horizontal_overlap.append(None)
            vertical_overlap.append(None)
            column_names.append(None)
            typeOfValues.append(None)

    return category, mother_table, way, horizontal_overlap, vertical_overlap, column_names, typeOfValues


def split_data_by_type(dataframe, config):
    jdf = dataframe[dataframe['Category'] == config['problem']]
    result = None

    for c in config["split"]:
        df = jdf.drop(c['drop_alg'], axis=1)

        if config['problem'] == "Joinable" or config['problem'] == "View-Unionable":
            df.loc[df['TypeOfValues'].isnull(), 'TypeOfValues'] = 'ev'
        else:
            df.loc[df['TypeOfValues'].isnull(), 'TypeOfValues'] = 'av'

        df.loc[df['ColumnNames'].isnull(), 'ColumnNames'] = 'ac'

        if c['type'] == 'Both':
            df['SplitType'] = [
                'Verbatim Instances/Schemata' if 'ec' in el['ColumnNames'] and el['TypeOfValues'] == 'ev'
                else 'Noisy Instances/Schemata' for i, el in df[['TypeOfValues', 'ColumnNames']].iterrows()]
        else:
            df['SplitType'] = df[c['type']]

            if c['type'] == "ColumnNames":
                df.loc[df['SplitType'] != 'ec', 'SplitType'] = 'Noisy Schemata'
                df.loc[df['SplitType'] == 'ec', 'SplitType'] = 'Verbatim Schemata'
            else:
                df.loc[df['SplitType'] == 'av', 'SplitType'] = 'Noisy Instances'
                df.loc[df['SplitType'] == 'ev', 'SplitType'] = 'Verbatim Instances'

        df = df.drop(c['drop'], axis=1)
        df1 = pd.melt(df, ['SplitType', 'MotherTable'], var_name="Algorithms")

        if result is None:
            result = df1
        else:
            result = pd.concat([result, df1])

    return result


def make_data_final_plot(dataframe, instance, schema, hybrid, config):
    data = split_data_by_type(dataframe, config)
    # app.logger.info(f"{data['Algorithms']}")
    d_inst = data[data['Algorithms'].isin(instance)]
    d_inst = d_inst.drop(d_inst[d_inst['MotherTable'] == 'Musicians'].index)  # No wikidata

    d_sch = data[data['Algorithms'].isin(schema)]
    d_sch = d_sch.drop(d_sch[d_sch['MotherTable'] == 'Musicians'].index)  # No wikidata

    d_hybrid = data[data['Algorithms'].isin(hybrid)]
    d_hybrid = d_hybrid.drop(d_hybrid[d_hybrid['MotherTable'] == 'Musicians'].index)  # No wikidata

    dedi = d_hybrid[d_hybrid['Algorithms'] == 'EmbDI']
    ds = d_hybrid[d_hybrid['Algorithms'] == 'SemProp']
    ds = ds[ds['MotherTable'] == 'assays']
    d_hybrid = pd.concat([dedi, ds])

    return d_inst, d_sch, d_hybrid


class ValentinePlots:
    def __init__(self):
        self.recall_at_sizeof_ground_truth = dict()
        self.total_metrics = dict()
        self.run_times = dict()
        self.f1_score = dict()
        self.best_rec_gnd_pd = None
        self.config = dict()

    def get_data(self):
        for key in self.total_metrics.keys():
            key_split = key.split('__')
            dataset_name = key_split[0]

            self.f1_score[dataset_name] = dict()
            self.recall_at_sizeof_ground_truth[dataset_name] = dict()

        for key in self.total_metrics.keys():
            key_split = key.split('__')
            dataset_name = key_split[0]
            algorithm_name = key_split[1].split('_')[0]

            if algorithm_name == "Coma":
                if "COMA_OPT_INST" in key:
                    algorithm_name = "COMA-SI"
                else:
                    algorithm_name = "COMA-S"
            elif algorithm_name == "CorrelationClustering":
                algorithm_name = "DistributionBased"

            if not "precision_at_n_percent" in self.total_metrics[key].keys():
                self.f1_score[dataset_name][algorithm_name] = dict()
                self.recall_at_sizeof_ground_truth[dataset_name][algorithm_name] = dict()

        for key in self.total_metrics.keys():
            key_split = key.split('__')
            dataset_name = key_split[0]
            algorithm_name = key_split[1].split('_')[0]
            parameters = key_split[1].split(algorithm_name)[1]

            if algorithm_name == "Coma":
                if "COMA_OPT_INST" in key:
                    algorithm_name = "COMA-SI"
                else:
                    algorithm_name = "COMA-S"
            elif algorithm_name == "CorrelationClustering":
                algorithm_name = "DistributionBased"

            if not "precision_at_n_percent" in self.total_metrics[key].keys():
                self.f1_score[dataset_name][algorithm_name][parameters] = self.total_metrics[key]['f1_score']
                self.recall_at_sizeof_ground_truth[dataset_name][algorithm_name][parameters] = self.total_metrics[key][
                    'recall_at_sizeof_ground_truth']

    def process_data(self):
        best_dict = {}
        for dataset in self.f1_score.keys():
            best_dict[dataset] = copy.deepcopy(algorithms)

        best_table = copy.deepcopy(best_dict)
        find_best_config_dict(self.f1_score, best_dict, best_table)

        best_prec_dict = copy.deepcopy(best_dict)
        get_best_metric(self.f1_score, best_prec_dict, best_dict)
        best_prec_pd = pd.DataFrame.from_dict(best_prec_dict, orient='index').reset_index().rename(
            columns={"index": "Dataset"})
        best_prec_pd.fillna(value=np.nan, inplace=True)

        category, mother_table, way, horizontal_overlap, vertical_overlap, column_names, typeOfValues \
            = parse_datasets(best_prec_pd)

        # app.logger.info(f"\nparse_dataset\n{category, mother_table, way, horizontal_overlap, vertical_overlap, column_names, typeOfValues}")

        nm_table = copy.deepcopy(best_dict)
        nm_dict = copy.deepcopy(best_dict)
        find_best_config_dict(self.recall_at_sizeof_ground_truth, nm_dict, nm_table)
        self.best_rec_gnd_pd = pd.DataFrame.from_dict(nm_table, orient='index').reset_index().rename(
            columns={"index": "Dataset"})
        self.best_rec_gnd_pd['Category'] = category
        self.best_rec_gnd_pd['MotherTable'] = mother_table
        self.best_rec_gnd_pd['SplitMethod'] = way
        self.best_rec_gnd_pd['HorizontalOverlap'] = horizontal_overlap
        self.best_rec_gnd_pd['VerticalOverlap'] = vertical_overlap
        self.best_rec_gnd_pd['ColumnNames'] = column_names
        self.best_rec_gnd_pd['TypeOfValues'] = typeOfValues
        self.best_rec_gnd_pd.fillna(value=pd.np.nan, inplace=True)

    def read_data(self):
        self.get_data()
        self.process_data()
        self.read_config_file()

    def create_box_plot(self):
        df = self.best_rec_gnd_pd.copy()

        big_df = dict()
        for c in self.config:
            big_df[c['problem']] = make_data_final_plot(df, instance_order, schema_order, hybrid_order, c)

        i = plot_by_data_type(big_df, 0, instance_order, instance_colors, hatches_inst, 'instance-based',
                                          hue_order_inst,
                                          map_instance)
        s = plot_by_data_type(big_df, 1, schema_order, schema_colors, hatches_sch, 'schema-based',
                                        hue_order_sch, map_schema)
        h = plot_by_data_type(big_df, 2, hybrid_order, hybrid_colors, hatches_hybrid, 'hybrid',
                                        hue_order_hybrid, map_hybrid)
        return i, s, h

    def read_config_file(self):
        with open("plot-config.json", "r") as fp:
            config = json.load(fp)
        self.config = config['config']
