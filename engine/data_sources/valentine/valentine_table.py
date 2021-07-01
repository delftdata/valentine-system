from typing import List, Dict
import pandas as pd
from minio import Minio

from .valentine_column import ValentineColumn
from ..base_column import BaseColumn
from ..base_table import BaseTable
from ..minio.minio_utils import get_dict_from_minio_json_file, get_pandas_df_from_minio_csv_file


class ValentineTable(BaseTable):

    def __init__(self, minio_client: Minio, instance_path: str, schema_path: str, dataset_name: str, dataset_group: str,
                 load_instances: bool):
        self.minio_client: Minio = minio_client
        self.__instance_path: str = instance_path
        self.__schema_path: str = schema_path
        self.__table_name: str = dataset_name  # dataset name
        self.__db_name: str = dataset_group  # dataset_group name
        self.__columns: dict[str, ValentineColumn] = dict()
        self.__load_schema()
        if load_instances:
            self.__load_instances()

    @property
    def unique_identifier(self) -> str:
        return f'{self.__db_name}__{self.__table_name}'

    @property
    def db_belongs_uid(self) -> str:
        return self.__db_name

    @property
    def name(self) -> str:
        return self.__table_name

    def get_columns(self) -> List[BaseColumn]:
        return list(self.__columns.values())

    @property
    def is_empty(self) -> bool:
        return len(list(self.__columns.keys())) == 0

    def get_tables(self, load_data: bool = True) -> Dict[str, BaseTable]:
        if load_data:
            self.__load_instances()
        return {self.name: self}

    def remove_table(self, guid: object) -> BaseTable:
        pass  # Since its a single table we cannot delete it (overridden from BaseDB)

    def add_table(self, table: BaseTable) -> None:
        pass  # Since its a single table we cannot add another table to it (overridden from BaseDB)

    def get_table_str_guids(self) -> List[str]:
        return [str(self.unique_identifier)]

    def __load_schema(self):
        schema = get_dict_from_minio_json_file(self.minio_client, 'fabricated_data', self.__schema_path)
        self.__columns = {column_name: ValentineColumn(column_name, metadata['type'], self.unique_identifier)
                          for column_name, metadata in schema.items()}

    def __load_instances(self):
        table_df: pd.DataFrame = get_pandas_df_from_minio_csv_file(self.minio_client,
                                                                   'fabricated_data', self.__instance_path)
        for (column_name, column_data) in table_df.iteritems():
            data = list(column_data.dropna().values)
            self.__columns[column_name].append_data(data)
