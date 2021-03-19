import pandas as pd

from engine.utils.utils import get_encoding, get_delimiter


def get_columns_from_local_fs_csv_file(table_path: str):
    return pd.read_csv(table_path, nrows=0).columns.tolist()


def get_pandas_df_from_local_fs_csv_file(table_path: str):
    return pd.read_csv(table_path,
                       index_col=False,
                       encoding=get_encoding(table_path),
                       sep=get_delimiter(table_path),
                       error_bad_lines=False)


def get_column_sample_from_csv_file(table_path: str, column_name: str, n: int):
    df = pd.read_csv(table_path,
                     usecols=[column_name],
                     nrows=2*n,
                     index_col=False,
                     encoding=get_encoding(table_path),
                     sep=get_delimiter(table_path),
                     error_bad_lines=False)
    sample = df[column_name].dropna().tolist()[:n]
    if len(sample) < n:
        sample = sample + [''] * (n - len(sample))
    return sample


def correct_file_ending(file_name: str):
    if file_name.endswith(".csv"):
        return file_name
    return file_name + ".csv"
