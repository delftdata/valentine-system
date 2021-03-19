import csv
import chardet
from minio import Minio
from io import BytesIO
import pandas as pd


def get_in_memory_encoding(f):
    encoding = chardet.detect(f)['encoding']
    if encoding == 'ascii':
        return 'utf-8'
    else:
        return encoding


def get_in_memory_delimiter(f):
    first_line = str(f).split('\\n')[0][2:]
    s = csv.Sniffer()
    return str(s.sniff(first_line).delimiter)


def get_columns_from_minio_csv_file(minio_client: Minio, bucket_name: str, object_name: str):
    data = minio_client.get_object(bucket_name, object_name)
    return pd.read_csv(BytesIO(list(data.stream(32 * 1024))[0]), nrows=0).columns.tolist()


def get_pandas_df_from_minio_csv_file(minio_client: Minio, bucket_name: str, object_name: str):
    obj_size = minio_client.stat_object(bucket_name, object_name).size
    data = list(minio_client.get_object(bucket_name, object_name).stream(obj_size))[0]
    return pd.read_csv(BytesIO(data),
                       index_col=False,
                       encoding=get_in_memory_encoding(data[:16 * 1024]),
                       sep=get_in_memory_delimiter(data[:16 * 1024]),
                       error_bad_lines=False)


def get_column_sample_from_minio_csv_file(minio_client: Minio, bucket_name: str, table_name: str, column_name: str,
                                          n: int):
    object_name = correct_file_ending(table_name)
    obj_size = minio_client.stat_object(bucket_name, object_name).size
    data = list(minio_client.get_object(bucket_name, object_name).stream(obj_size))[0]
    df = pd.read_csv(BytesIO(data),
                     usecols=[column_name],
                     nrows=2*n,
                     index_col=False,
                     encoding=get_in_memory_encoding(data[:16 * 1024]),
                     sep=get_in_memory_delimiter(data[:16 * 1024]),
                     error_bad_lines=False)
    sample = df[column_name].dropna().tolist()[:n]
    if len(sample) < n:
        sample = sample + [''] * (n - len(sample))
    return sample


def correct_file_ending(file_name: str):
    if file_name.endswith(".csv"):
        return file_name
    return file_name + ".csv"
