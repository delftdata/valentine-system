import configparser


def str_to_bool(s):
    if s == 'True':
        return True
    elif s == 'False':
        return False
    else:
        raise ValueError


class Configuration:

    def __init__(self, cfgfile: str):
        cfg = configparser.ConfigParser(interpolation=configparser.ExtendedInterpolation())
        cfg.read(cfgfile)
        self.home_dir = cfg['Paths']['home_dir']
        self.input_dir = cfg['Paths']['input_dir']
        self.output_dir = cfg['Paths']['output_dir']

        self.source_data = cfg['Files']['source_data']
        self.source_schema = cfg['Files']['source_schema']
        self.target_names = cfg['Files']['output_files']

        self.overlap = int(cfg['Properties']['overlap'])
        self.random_overlap = str_to_bool(cfg['Properties']['random_overlap'])
        self.vertical_split = str_to_bool(cfg['Properties']['vertical_split'])
        self.horizontal_split = str_to_bool(cfg['Properties']['horizontal_split'])

        self.primary_key = int(cfg['Columns']['PK'])
        self.split_pk = str_to_bool(cfg['Columns']['split_pk'])
        self.col_split_random = str_to_bool(cfg['Columns']['split_random'])
        self.columns = int(cfg['Columns']['columns'])

        self.approx = str_to_bool(cfg['Approximation']['approx'])
        self.prc = int(cfg['Approximation']['percentage'])
        self.approx_prc = int(cfg['Approximation']['approx_percentage'])
        self.approx_columns = str_to_bool(cfg['Approximation']['approx_columns'])
        self.approx_columns_type = int(cfg['Approximation']['approx_columns_type'])

