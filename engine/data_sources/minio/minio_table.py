from typing import List, Dict
import pandas as pd
from minio import Minio

from .minio_column import MinioColumn
from .minio_utils import get_columns_from_minio_csv_file, get_pandas_df_from_minio_csv_file, correct_file_ending
from ..base_column import BaseColumn
from ..base_table import BaseTable
from ...utils.utils import is_date


class MinioTable(BaseTable):

    def __init__(self, minio_client: Minio, table_name: str, db_name: str, load_data: bool):
        self.minio_client = minio_client
        self.__table_name = table_name  # file name
        self.__db_name = db_name  # bucket name
        self.__columns = dict()
        self.__column_names = self.__get_column_names()
        if load_data:
            self.__get_columns_from_local_minio_tmp_copy()

    def __str__(self):
        __str: str = "\tTable: " + self.name + "  |  " + str(self.unique_identifier) + "\n"
        for column in self.get_columns():
            __str = __str + str(column.__str__())
        return __str

    @property
    def unique_identifier(self) -> tuple:
        return self.__db_name, self.__table_name

    @property
    def db_belongs_uid(self) -> object:
        return self.__db_name

    @property
    def name(self) -> str:
        return correct_file_ending(self.__table_name).replace('/', '_').split('.')[0]

    def get_columns(self) -> List[BaseColumn]:
        if not self.__columns:
            self.__get_columns_from_local_minio_tmp_copy()
        return list(self.__columns.values())

    def get_tables(self, load_data: bool = True) -> Dict[str, BaseTable]:
        if not self.__columns:
            if load_data:
                self.__get_columns_from_local_minio_tmp_copy()
            else:
                column_names: List[str] = self.__get_column_names()
                self.__columns = {column_name: MinioColumn(column_name, [], 'NULL', self.unique_identifier)
                                  for column_name in column_names}
        return {self.name: self}

    def get_table_str_guids(self) -> List[str]:
        return [str(self.unique_identifier)]

    def remove_table(self, guid: object) -> BaseTable:
        pass  # Since its a single table we cannot delete it (overridden from BaseDB)

    def add_table(self, table: BaseTable) -> None:
        pass  # Since its a single table we cannot add another table to it (overridden from BaseDB)

    @property
    def is_empty(self) -> bool:
        return len(self.__column_names) == 0

    def get_table_guids(self) -> List[object]:
        return [self.unique_identifier]

    def __get_column_names(self) -> List[str]:
        return get_columns_from_minio_csv_file(self.minio_client, self.__db_name, self.__table_name)

    def __get_columns_from_local_minio_tmp_copy(self):
        table_df: pd.DataFrame = get_pandas_df_from_minio_csv_file(self.minio_client, self.__db_name, self.__table_name)
        for (column_name, column_data) in table_df.iteritems():
            d_type = str(column_data.dtype)
            data = list(column_data.dropna().values)
            if len(data) != 0:
                d_type = self.__get_true_data_type(d_type, data)
                self.__columns[column_name] = MinioColumn(column_name, data, d_type, self.unique_identifier)
            else:
                if d_type == "object":
                    self.__columns[column_name] = MinioColumn(column_name, data, "varchar", self.unique_identifier)
                else:
                    self.__columns[column_name] = MinioColumn(column_name, data, d_type, self.unique_identifier)

    @staticmethod
    def __get_true_data_type(d_type, data):
        if d_type == "object":
            if is_date(data[0]):
                d_type = "date"
            else:
                d_type = "varchar"
        elif d_type.startswith("int"):
            d_type = "int"
        elif d_type.startswith("float"):
            d_type = "float"
        return d_type
