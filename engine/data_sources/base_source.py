from abc import ABC, abstractmethod
from typing import Dict, Union, List

from .base_db import BaseDB
from .base_table import BaseTable


class Error(Exception):
    pass


class GUIDMissing(Error):
    pass


class BaseSource(ABC):
    """
    Abstract class representing a source (i.e. Atlas, Minio)
    """

    @abstractmethod
    def contains_db(self, guid: object) -> bool:
        raise NotImplementedError

    @abstractmethod
    def get_db(self, guid: object, load_data: bool = True) -> BaseDB:
        raise NotImplementedError

    @abstractmethod
    def get_all_dbs(self, load_data: bool = True) -> Dict[object, BaseDB]:
        raise NotImplementedError

    @abstractmethod
    def get_db_table(self, guid: object, db_guid: object = None, load_data: bool = True) -> Union[BaseDB, BaseTable]:
        raise NotImplementedError

    @abstractmethod
    def get_column_sample(self, db_name: str, table_name: str, column_name: str, n: int = 10) -> List:
        raise NotImplementedError
