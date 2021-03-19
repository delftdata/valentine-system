from itertools import repeat
from typing import Dict, Union, List
from multiprocessing.pool import ThreadPool

import requests

from .atlas_database import AtlasDatabase
from .atlas_table import AtlasTable
from .atlas_utils import get_entity_with_guid
from ..base_source import BaseSource, GUIDMissing


class AtlasSource(BaseSource):

    def __init__(self, url: str, user_name: str, password: str, db_types: list, parallelism: int, chunk_size: int):
        self.__db_types = db_types
        self.__url = url

        self.__auth = (user_name, password)

        self.__parallelism = parallelism
        self.__chunk_size = chunk_size

        self.__db_guids = list()
        self.__get_atlas_db_guids()

        self.__schemata = dict()

    def get_db(self, guid: str, load_data: bool = True) -> AtlasDatabase:
        if guid not in self.__db_guids:
            raise GUIDMissing

        if guid not in self.__schemata:
            self.__schemata[guid] = AtlasDatabase(self.__url, self.__auth, guid, self.__parallelism, self.__chunk_size)

        return self.__schemata[guid]

    def contains_db(self, guid: object) -> bool:
        pass

    def get_all_dbs(self, load_data: bool = True) -> Dict[object, AtlasDatabase]:
        with ThreadPool(self.__parallelism) as process_pool:
            self.__schemata = dict(process_pool.starmap(self.parallel_source_initialization,
                                                        zip(self.__db_guids, repeat(self.__url), repeat(self.__auth),
                                                            repeat(self.__parallelism), repeat(self.__chunk_size))))
        return self.__schemata

    def get_db_table(self, guid: object, db_guid: object = None,
                     load_data: bool = True) -> Union[AtlasDatabase, AtlasTable]:
        atlas_table = get_entity_with_guid(self.__url, self.__auth, str(guid))['entity']
        if 'technologyPlatform' in atlas_table['attributes']:
            technology = atlas_table['attributes']['technologyPlatform']
            db_guid = atlas_table['attributes']['cluster']['guid']
        else:
            technology = get_entity_with_guid(self.__url, self.__auth,
                                              atlas_table['relationshipAttributes']['database']['guid']
                                              )['entity']['attributes']['technologyPlatform']
            db_guid = atlas_table['attributes']['database']['guid']
        column_guids = []
        columns = []
        if technology == "mssql":
            columns = atlas_table['relationshipAttributes']['columns']
        elif technology == "Elasticsearch":
            columns = atlas_table["relationshipAttributes"]["fields"]
        for column in columns:
            column_guids.append(column['guid'])
        return AtlasTable(self.__url, self.__auth, atlas_table['attributes']['name'], str(guid), column_guids,
                          technology, db_guid, self.__chunk_size)

    def get_column_sample(self, db_name: str, table_name: str, column_name: str, n: int = 10) -> List:
        return ["Schema only source"] * n

    def __get_atlas_db_guids(self):
        for db_type in self.__db_types:
            r = requests.get(self.__url + '/api/atlas/v2/search/basic',
                             auth=self.__auth,
                             params={"typeName": db_type},
                             verify=False)
            for entity in r.json()["entities"]:
                self.__db_guids.append(entity['guid'])

    @staticmethod
    def parallel_source_initialization(db_guid, url, auth, parallelism, chunk_size):
        return db_guid, AtlasDatabase(url, auth, db_guid, parallelism, chunk_size)
