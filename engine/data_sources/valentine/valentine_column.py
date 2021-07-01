from ..base_column import BaseColumn


class ValentineColumn(BaseColumn):

    def __init__(self, column_name: str, d_type: str, table_guid: str):
        self.__column_name = column_name
        self.__data = None
        self.__d_type = d_type
        self.__table_guid = table_guid

    @property
    def unique_identifier(self) -> str:
        return self.__table_guid + ":" + self.__column_name

    @property
    def name(self):
        return self.__column_name

    @property
    def data_type(self):
        return self.__d_type

    @property
    def data(self) -> list:
        return self.__data

    def append_data(self, data: list):
        self.__data = list(filter(lambda d: d != '', data))
