import os
from typing import Union, Dict, List

from .postgres_database import PostgresDatabase
from .postgres_table import PostgresTable
from .postgres_utils import list_postgres_dbs, get_column_sample_from_postgres_table
from ..base_db import BaseDB
from ..base_source import BaseSource, GUIDMissing


class PostgresSource(BaseSource):

    def __init__(self):
        self.__postgres_url = f"postgresql+psycopg2://"\
                              f"{os.environ['POSTGRES_USER']}:"\
                              f"{os.environ['POSTGRES_PASSWORD']}@"\
                              f"{os.environ['POSTGRES_HOST']}:"\
                              f"{os.environ['POSTGRES_PORT']}"
        self.__dbs: Dict[object, BaseDB] = dict()
        self.__db_guids: list[str] = list_postgres_dbs(self.__postgres_url)

    def contains_db(self, guid: str) -> bool:
        if guid in self.__db_guids:
            return True
        return False

    def get_db(self, guid: str, load_data: bool = True) -> BaseDB:
        if guid not in self.__db_guids:
            raise GUIDMissing
        if guid not in self.__dbs:
            self.__dbs[guid] = PostgresDatabase(self.__postgres_url, guid)
        return self.__dbs[guid]

    def get_all_dbs(self, load_data: bool = True) -> Dict[object, BaseDB]:
        for db_name in self.__db_guids:
            self.__dbs[db_name] = PostgresDatabase(self.__postgres_url, db_name)
        return self.__dbs

    def get_db_table(self, guid: str, db_guid: str = None,
                     load_data: bool = True) -> Union[PostgresDatabase, PostgresTable]:
        if db_guid is None:
            raise GUIDMissing
        table: PostgresTable = PostgresTable(self.__postgres_url, guid, db_guid, load_data)
        return table

    def get_column_sample(self, db_name: str, table_name: str, column_name: str, n: int = 10) -> List:
        return get_column_sample_from_postgres_table(self.__postgres_url, db_name, table_name, column_name, n)
