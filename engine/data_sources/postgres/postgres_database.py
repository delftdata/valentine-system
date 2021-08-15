from typing import Dict, List

from .postgres_table import PostgresTable
from .postgres_utils import list_postgres_db_tables
from ..base_db import BaseDB
from ..base_table import BaseTable


class PostgresDatabase(BaseDB):

    def __init__(self, postgres_url: str, db_name: str):
        self.__postgres_url = postgres_url
        self.__db_name: str = db_name
        self.__tables: Dict[object, BaseTable] = dict()
        self.__get_tables_from_postgres(load_data=False)

    @property
    def unique_identifier(self) -> str:
        return self.__db_name

    @property
    def name(self) -> str:
        return self.__db_name

    def get_tables(self, load_data: bool = True) -> Dict[str, BaseTable]:
        self.__get_tables_from_postgres(load_data)
        tables: Dict[str, BaseTable] = {val.name: val for val in self.__tables.values() if not val.is_empty}
        return tables

    def remove_table(self, guid: str) -> BaseTable:
        table_to_be_removed = self.__tables[guid]
        del self.__tables[guid]
        return table_to_be_removed

    def add_table(self, table: BaseTable) -> None:
        self.__tables[table.unique_identifier] = table

    def get_table_str_guids(self) -> List[tuple]:
        return list(map(lambda x: x.unique_identifier, self.__tables.values()))

    @property
    def is_empty(self) -> bool:
        return len(list_postgres_db_tables(self.__postgres_url, self.__db_name)) == 0

    def __get_tables_from_postgres(self, load_data: bool):
        tables = list_postgres_db_tables(self.__postgres_url, self.__db_name)
        for table_name in tables:
            self.__tables[table_name] = PostgresTable(self.__postgres_url, table_name, self.__db_name, load_data)
