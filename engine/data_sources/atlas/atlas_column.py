from ..base_column import BaseColumn


class AtlasColumn(BaseColumn):

    def __init__(self, column_name: str, column_type: str, guid: str):
        self.__column_name = column_name
        self.__guid = guid
        self.__d_type = column_type

    @property
    def unique_identifier(self) -> object:
        return self.__guid

    @property
    def name(self):
        return self.__column_name

    @property
    def data_type(self):
        return self.__d_type

    @property
    def data(self) -> list:
        raise ValueError('Atlas does not support data instance retrieval!')
