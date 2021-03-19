from typing import List, Union, Dict
import os

from engine.data_sources.base_db import BaseDB
from engine.data_sources.base_source import BaseSource, GUIDMissing
from engine.data_sources.base_table import BaseTable
from engine.data_sources.local_fs.local_fs_database import LocalFSDatabase
from engine.data_sources.local_fs.local_fs_table import LocalFSTable
from engine.data_sources.local_fs.local_fs_utils import get_column_sample_from_csv_file, correct_file_ending


class LocalFsSource(BaseSource):

    def __init__(self, base_folder_path: str):
        self.__base_folder_path = base_folder_path
        self.__dbs: Dict[object, BaseDB] = dict()
        self.__db_guids: list = list(filter(lambda x: x != self.__base_folder_path.split(os.sep)[-1],
                                            [x[0].split(os.sep)[-1] for x in os.walk(self.__base_folder_path)]))

    def contains_db(self, guid: object) -> bool:
        if guid in self.__db_guids:
            return True
        return False

    def get_db(self, guid: object, load_data: bool = True) -> BaseDB:
        if guid not in self.__db_guids:
            raise GUIDMissing
        if guid not in self.__dbs:
            self.__dbs[guid] = LocalFSDatabase(self.__base_folder_path, str(guid))
        return self.__dbs[guid]

    def get_all_dbs(self, load_data: bool = True) -> Dict[object, BaseDB]:
        for db_name in self.__db_guids:
            self.__dbs[db_name] = LocalFSDatabase(self.__base_folder_path, db_name)
        return self.__dbs

    def get_db_table(self, guid: object, db_guid: object = None, load_data: bool = True) -> Union[BaseDB, BaseTable]:
        if db_guid is None:
            raise GUIDMissing
        try:
            table_path = self.__base_folder_path + os.sep + str(db_guid) + os.sep + correct_file_ending(str(guid))
            table: LocalFSTable = LocalFSTable(table_path, correct_file_ending(str(guid)), str(db_guid),
                                               load_data)
        except FileNotFoundError:
            raise GUIDMissing
        else:
            return table

    def get_column_sample(self, db_name: str, table_name: str, column_name: str, n: int = 10) -> List:
        table_path = self.__base_folder_path + os.sep + db_name + os.sep + correct_file_ending(table_name)
        return get_column_sample_from_csv_file(table_path, column_name, n)
