import pandas as pd
from typing import Union

from src.transformer.data_transformer import DataTransformer
from src.database.models.country_mapping import CountryMapping


class XtbTransformer(DataTransformer):
    def __init__(self,
                 names_mapping: Union[list, None] = None):
        super().__init__()

        self._names_mapping_raw = names_mapping

    @property
    def names_mapping_raw(self):
        return self._names_mapping_raw

    @names_mapping_raw.setter
    def names_mapping_raw(self, value):
        self._names_mapping_raw = value

    def process_data(self,
                     names_mapping: Union[list, None] = None):
        self._names_mapping_raw = names_mapping if names_mapping is not None else self._names_mapping_raw

        names = self.process_names_data(self._names_mapping_raw)
        return names

    def process_names_data(self, raw_data: Union[list, None] = None):
        self._names_mapping_raw = raw_data if raw_data is not None else self._names_mapping_raw

        names = list()
        for elem in self._names_mapping_raw:
            elem['ticker'] = elem.pop('symbol')
            names.append(elem)

        cols = [col.name for col in CountryMapping.__table__.columns]
        names_df = pd.DataFrame(names, columns=cols)
        names_df.drop_duplicates(subset=['ticker'], inplace=True)
        return names_df

