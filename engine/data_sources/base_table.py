from abc import ABC, abstractmethod
from typing import List, Dict
from cached_property import cached_property

from .base_column import BaseColumn
from .base_db import BaseDB


class BaseTable(BaseDB, ABC):
    """
    Abstract class representing a table
    """

    @property
    @abstractmethod
    def unique_identifier(self) -> object:
        raise NotImplementedError

    @property
    @abstractmethod
    def db_belongs_uid(self) -> object:
        raise NotImplementedError

    @property
    @abstractmethod
    def name(self) -> str:
        raise NotImplementedError

    @abstractmethod
    def get_columns(self) -> List[BaseColumn]:
        raise NotImplementedError

    @property
    @abstractmethod
    def is_empty(self) -> bool:
        raise NotImplementedError

    @cached_property
    def get_guid_column_lookup(self) -> Dict[str, object]:
        return {column.name:  column.unique_identifier for column in self.get_columns()}
