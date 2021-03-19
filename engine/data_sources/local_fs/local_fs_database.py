import os
from typing import List, Dict

from engine.data_sources.base_db import BaseDB
from engine.data_sources.base_table import BaseTable
from engine.data_sources.local_fs.local_fs_table import LocalFSTable
from engine.data_sources.local_fs.local_fs_utils import correct_file_ending


class LocalFSDatabase(BaseDB):

    def __init__(self, base_folder_path: str, db_guid: str):
        self.__base_folder_path: str = base_folder_path
        self.__db_name: str = db_guid
        self.__tables: Dict[object, BaseTable] = dict()
        self.__get_tables_from_local_fs(load_data=False)

    @property
    def unique_identifier(self) -> object:
        return self.__db_name

    @property
    def name(self) -> str:
        return self.__db_name

    def get_tables(self, load_data: bool = True) -> Dict[str, BaseTable]:
        self.__get_tables_from_local_fs(load_data)
        tables: Dict[str, BaseTable] = {val.name: val for val in self.__tables.values() if not val.is_empty}
        return tables

    def remove_table(self, guid: object) -> BaseTable:
        guid = correct_file_ending(str(guid))
        table_to_be_removed = self.__tables[guid]
        del self.__tables[guid]
        return table_to_be_removed

    def add_table(self, table: BaseTable) -> None:
        self.__tables[table.unique_identifier] = table

    def get_table_str_guids(self) -> List[str]:
        return list(map(lambda x: x.unique_identifier, self.__tables.values()))

    @property
    def is_empty(self) -> bool:
        if os.path.exists(self.__base_folder_path) and os.path.isdir(self.__base_folder_path):
            if not os.listdir(self.__base_folder_path):
                return True
            else:
                return False
        else:
            return True

    def __get_tables_from_local_fs(self, load_data: bool):
        for (_, _, filenames) in os.walk(self.__base_folder_path + os.sep + self.__db_name):
            for filename in filenames:
                table_path = self.__base_folder_path + os.sep + self.__db_name + os.sep + filename
                self.__tables[filename] = LocalFSTable(table_path, filename, self.__db_name, load_data)
