from abc import ABC, abstractmethod

from typing import Dict, List


class BaseDB(ABC):
    """
    Abstract class representing a database
    """

    @property
    @abstractmethod
    def unique_identifier(self) -> object:
        raise NotImplementedError

    @property
    @abstractmethod
    def name(self) -> str:
        raise NotImplementedError

    @abstractmethod
    def get_tables(self, load_data: bool = True) -> Dict[str, 'BaseDB']:
        raise NotImplementedError

    @abstractmethod
    def remove_table(self, guid: object) -> 'BaseDB':
        raise NotImplementedError

    @abstractmethod
    def add_table(self, table: 'BaseDB') -> None:
        raise NotImplementedError

    def get_table_str_guids(self) -> List[str]:
        raise NotImplementedError

    @property
    @abstractmethod
    def is_empty(self) -> bool:
        raise NotImplementedError

    def number_of_tables(self) -> int:
        return len(self.get_tables(load_data=False))

    def __str__(self):
        __str = "DB: " + self.name + "  |  " + str(self.unique_identifier) + "\n"
        for table in self.get_tables().values():
            __str = __str + table.__str__()
        return __str
