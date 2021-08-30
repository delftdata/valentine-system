import json
import os
import tempfile
from zipfile import ZipFile

from minio import Minio
from io import BytesIO
import pandas as pd

from engine import ValentineLoadDataError
from engine.utils.utils import get_in_memory_encoding, get_in_memory_delimiter, is_date


def get_columns_from_minio_csv_file(minio_client: Minio, bucket_name: str, object_name: str):
    data = minio_client.get_object(bucket_name, object_name)
    return pd.read_csv(BytesIO(list(data.stream(32 * 1024))[0]), nrows=0).columns.tolist()


def get_pandas_df_from_minio_csv_file(minio_client: Minio, bucket_name: str, object_name: str,
                                      skiprows=None, header='infer'):
    try:
        obj_size = minio_client.stat_object(bucket_name, object_name).size
        data = list(minio_client.get_object(bucket_name, object_name).stream(obj_size))[0]
        df = pd.read_csv(BytesIO(data),
                         index_col=False,
                         skiprows=skiprows,
                         header=header,
                         encoding=get_in_memory_encoding(data[:16 * 1024]),
                         sep=get_in_memory_delimiter(data[:16 * 1024]),
                         on_bad_lines='warn',
                         encoding_errors='ignore')
    except Exception:
        raise ValentineLoadDataError
    else:
        return df


def get_valentine_schema_from_minio_csv_file(minio_client: Minio, bucket_name: str, object_name: str) -> dict:
    try:
        obj_size = minio_client.stat_object(bucket_name, object_name).size
        data = list(minio_client.get_object(bucket_name, object_name).stream(obj_size))[0]
        df = pd.read_csv(BytesIO(data),
                         index_col=False,
                         encoding=get_in_memory_encoding(data[:16 * 1024]),
                         sep=get_in_memory_delimiter(data[:16 * 1024]),
                         on_bad_lines='warn',
                         encoding_errors='ignore')
        i = 0
        schema = {}
        for (column_name, column_data) in df.iteritems():
            d_type = str(column_data.dtype)
            data = list(column_data.dropna().values)
            if len(data) != 0 and d_type == "object" and is_date(data[0]):
                d_type = "date"
            elif len(data) != 0 and d_type == "object":
                d_type = "str"
            elif len(data) == 0 and d_type == "object":
                d_type = "str"
            schema[str(i)] = [column_name, d_type]
            i += 1
    except Exception:
        raise ValentineLoadDataError
    else:
        return schema


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
                     on_bad_lines='warn',
                     encoding_errors='ignore')
    sample = df[column_name].dropna().tolist()[:n]
    if len(sample) < n:
        sample = sample + [''] * (n - len(sample))
    return sample


def get_column_sample_from_minio_csv_file2(minio_client: Minio, bucket_name: str,
                                           file_path: str, n: int) -> tuple[list[str], list[object]]:
    obj_size = minio_client.stat_object(bucket_name, file_path).size
    data = list(minio_client.get_object(bucket_name, file_path).stream(obj_size))[0]
    df: pd.DataFrame = pd.read_csv(BytesIO(data),
                                   index_col=False,
                                   nrows=n,
                                   encoding=get_in_memory_encoding(data[:16 * 1024]),
                                   sep=get_in_memory_delimiter(data[:16 * 1024]),
                                   on_bad_lines='warn',
                                   encoding_errors='ignore').fillna('')
    return list(df.columns), list(df.values)


def get_dict_from_minio_json_file(minio_client: Minio, bucket_name: str, object_path: str) -> dict:
    try:
        obj_size = minio_client.stat_object(bucket_name, object_path).size
        data = list(minio_client.get_object(bucket_name, object_path).stream(obj_size))[0]
        python_dict = json.load(BytesIO(data))
    except Exception:
        raise ValentineLoadDataError
    else:
        return python_dict


def correct_file_ending(file_name: str):
    if file_name.endswith(".csv"):
        return file_name
    return file_name + ".csv"


def list_bucket_files(bucket_name: str, prefix: str, minio_client: Minio) -> dict[str, dict[str, list[str]]]:
    bucket_files = minio_client.list_objects(bucket_name, prefix=prefix + os.path.sep, recursive=True)
    folders: dict[str, dict[str, list[str]]] = {}
    for file in bucket_files:
        split_file_path: list[str] = file.object_name.split(os.path.sep)
        root_folder: str = split_file_path[0]  # bucket name
        scenario_folder: str = split_file_path[1]  # scenario (e.g., Joinable)
        pair_folder: str = split_file_path[2]
        top_level_folder = os.path.join(root_folder, scenario_folder)
        if top_level_folder in folders:
            if pair_folder in folders[top_level_folder]:
                folders[top_level_folder][pair_folder].append(file.object_name)
            else:
                folders[top_level_folder][pair_folder] = [file.object_name]
        else:
            folders[top_level_folder] = {pair_folder: [file.object_name]}
    return folders


def store_dict_to_minio_as_json(minio_client: Minio, d: dict, bucket_name: str, file_name: str):
    output = json.dumps(d, indent=2).encode('utf-8')
    minio_client.put_object(
        bucket_name=bucket_name,
        object_name=file_name,
        data=BytesIO(output),
        length=len(output)
    )


def download_zipped_data_from_minio(minio_client: Minio,
                                    bucket: str,
                                    files_to_download: list,
                                    dataset_group_name: str) -> str:
    tmp_dir_path = tempfile.gettempdir()
    zip_path = os.path.join(tmp_dir_path, dataset_group_name)
    os.makedirs(zip_path, exist_ok=True)
    zip_path = os.path.join(zip_path, f'{dataset_group_name}.zip')
    with ZipFile(zip_path, 'w') as zip_object:
        for fabricated_dataset_pair in files_to_download:
            file_path = os.path.join(tmp_dir_path, fabricated_dataset_pair.object_name)
            minio_client.fget_object(bucket,
                                     fabricated_dataset_pair.object_name,
                                     file_path)
            zip_object.write(file_path, fabricated_dataset_pair.object_name)
    return zip_path
