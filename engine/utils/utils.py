import calendar
import csv
import hashlib
import time
from pathlib import Path
import os
import openpyxl
from dateutil.parser import parse
import chardet
from flask.json import JSONEncoder
import numpy as np
from minio import Minio


class ValentineJsonEncoder(JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        elif isinstance(obj, np.floating):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        else:
            return super(JSONEncoder, self).default(obj)


def is_sorted(matches: dict):
    prev = None
    for value in matches.values():
        if prev is None:
            prev = value
        else:
            if prev > value:
                return False
    return True


def convert_data_type(string: str):
    try:
        f = float(string)
        if f.is_integer():
            return int(f)
        return f
    except ValueError:
        return string


def get_project_root():
    return str(Path(__file__).parent.parent)


def create_folder(path: str):
    try:
        os.makedirs(path)
    except (OSError, FileExistsError):
        pass


def allowed_csv_file(filename: str):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() == 'csv'


def allowed_xlsx_file(filename: str):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() == 'xlsx'


def xlsx_to_csvs(xlsx_file_path: str, db_name: str, store_path: str):
    """
    Function that transforms an xlsx file to its corresponding csv files in the csv_file_store
    :param xlsx_file_path: str the xlsx file path
    :param db_name: str the database name that it represents
    :param store_path: str the path of the csv file store
    """
    wb_obj = openpyxl.load_workbook(xlsx_file_path)
    folder_path = store_path+os.path.sep+db_name
    create_folder(folder_path)
    for sheet in wb_obj.worksheets:
        data = sheet.rows
        with open(folder_path+os.path.sep+sheet.title+".csv", "w+", encoding=sheet.encoding) as csv_file:
            for row in data:
                row_data = list(row)
                for i, entry in enumerate(row_data):
                    if i == len(row_data) - 1:
                        csv_file.write(str(entry.value))
                    else:
                        csv_file.write(str(entry.value) + ',')
                csv_file.write('\n')


def directory_tree(dir_path: Path, prefix: str = ''):
    """
    A recursive generator, given a directory Path object
    will yield a visual tree structure line by line
    with each line prefixed by the same characters
    """
    space = '░░░░'
    branch = '│░░░'
    tee = '├──>'
    last = '└──>'
    contents = list(dir_path.iterdir())
    pointers = [tee] * (len(contents) - 1) + [last]
    for pointer, path in zip(pointers, contents):
        yield prefix + pointer + path.name
        if path.is_dir():
            extension = branch if pointer == tee else space
            yield from directory_tree(path, prefix=prefix+extension)


def get_encoding(ds_path: str) -> str:
    """ Returns the encoding of the file """
    test_str = b''
    number_of_lines_to_read = 500
    count = 0
    with open(ds_path, 'rb') as f:
        line = f.readline()
        while line and count < number_of_lines_to_read:
            test_str = test_str + line
            count += 1
            line = f.readline()
        result = chardet.detect(test_str)
    if result['encoding'] == 'ascii':
        return 'utf-8'
    else:
        return result['encoding']


def get_delimiter(ds_path: str) -> str:
    """ Returns the delimiter of the csv file """
    with open(ds_path) as f:
        first_line = f.readline()
        s = csv.Sniffer()
        return str(s.sniff(first_line).delimiter)


def is_date(string, fuzzy=False):
    """
    Return whether the string can be interpreted as a date.
    :param string: str, string to check for date
    :param fuzzy: bool, ignore unknown tokens in string if True
    """
    try:
        parse(str(string), fuzzy=fuzzy)
        return True
    except Exception:
        return False


def get_timestamp() -> str:
    return str(calendar.timegm(time.gmtime()))


def get_sha1_hash_of_string(string: str) -> str:
    return str(hashlib.sha1(string.encode()).hexdigest())


def delete_file(path: str):
    if os.path.exists(path):
        os.remove(path)


def init_minio_buckets(minio_client: Minio, bucket_names: list[str]):
    for bucket_name in bucket_names:
        if not minio_client.bucket_exists(bucket_name):
            minio_client.make_bucket(bucket_name)


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
