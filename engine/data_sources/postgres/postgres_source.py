import os
from typing import Union, Dict, List

from ..base_db import BaseDB
from ..base_source import BaseSource, GUIDMissing


class PostgresSource(BaseSource):

    def __init__(self):
        # self.__minio_client = Minio('{host}:{port}'.format(host=os.environ['MINIO_HOST'],
        #                                                    port=os.environ['MINIO_PORT']),
        #                             access_key=os.environ['MINIO_ACCESS_KEY'],
        #                             secret_key=os.environ['MINIO_SECRET_KEY'],
        #                             secure=False)
        # self.__dbs: Dict[object, BaseDB] = dict()
        # self.__db_guids: list = list(map(lambda bucket: bucket.name, self.__minio_client.list_buckets()))
        pass

    def contains_db(self, guid: object) -> bool:
        # if guid in self.__db_guids:
        #     return True
        # return False
        pass

    def get_db(self, guid: object, load_data: bool = True) -> BaseDB:
        pass
        # if guid not in self.__db_guids:
        #     raise GUIDMissing
        # if guid not in self.__dbs:
        #     self.__dbs[guid] = MinioDatabase(self.__minio_client, str(guid))
        # return self.__dbs[guid]

    def get_all_dbs(self, load_data: bool = True) -> Dict[object, BaseDB]:
        # for db_name in self.__db_guids:
        #     self.__dbs[db_name] = MinioDatabase(self.__minio_client, db_name)
        # return self.__dbs
        pass

    def get_db_table(self, guid: object, db_guid: object = None,
                     load_data: bool = True):  # -> Union[MinioDatabase, MinioTable]:
        pass
        # if db_guid is None:
        #     raise GUIDMissing
        # try:
        #     table: MinioTable = MinioTable(self.__minio_client, correct_file_ending(str(guid)), str(db_guid), load_data)
        # except MinioException:
        #     raise GUIDMissing
        # else:
        #     return table

    def get_column_sample(self, db_name: str, table_name: str, column_name: str, n: int = 10) -> List:
        pass
        # return get_column_sample_from_minio_csv_file(self.__minio_client, db_name, table_name, column_name, n)
