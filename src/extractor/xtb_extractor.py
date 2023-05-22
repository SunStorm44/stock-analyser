from typing import Union
import re

import src.xtb.xAPIConnector as xtbWrapper
from .data_extractor import DataExtractor


class XtbExtractor(DataExtractor):
    def __init__(self, auto_login=False):
        super().__init__()

        self.xtb_client = xtbWrapper.APIClient()
        self.xtb_session_id = None
        self._raw_data: Union[list, None] = None

        if auto_login:
            self.connect()

    @property
    def raw_data(self):
        return self._raw_data

    def connect(self):
        try:
            resp = self.xtb_client.execute(xtbWrapper.loginCommand(self.credentials['primary_user'],
                                                                   self.credentials['xtb'])
                                           )
            if not resp['status']:
                raise ConnectionError(f"Could not establish connection with XTB. Reason: {resp['errorCode']}")

            self.xtb_session_id = resp['streamSessionId']
        except Exception as e:
            raise ConnectionError(f'Could not establish connection with XTB. Reason: {e}')

    def disconnect(self):
        resp = self.xtb_client.commandExecute('logout')
        if resp['status']:
            print(f'Successfully logged-out from XTB API!')

    @staticmethod
    def __filter_categories(data: list,
                            category: str):
        unique_cat = set([elem['categoryName'] for elem in data])
        if category not in unique_cat:
            raise ValueError(f"Error, {', '.join(unique_cat)} are valid categories. {category} passed.")
        data = [elem for elem in data if elem['categoryName'] == category]
        return data

    def _get_raw_symbols(self) -> list:
        try:
            resp = self.xtb_client.commandExecute('getAllSymbols')
        except Exception as e:
            raise BrokenPipeError(f'Could not retrieve data from XTB API. Reason: {e}')
        else:
            self._raw_data = resp['returnData']
            return self._raw_data

    def get_data(self,
                 category: str = None,
                 include_cfd: bool = False,
                 group_by_country: bool = True) -> tuple:

        symbols = self.get_symbols(category, include_cfd, group_by_country)
        names = self.get_names(category)
        return symbols, names

    def get_symbols(self,
                    category: str = None,
                    include_cfd: bool = False,
                    group_by_country: bool = True) -> dict:

        data = self._raw_data if self._raw_data is not None else self._get_raw_symbols()

        if category is not None:
            data = self.__filter_categories(data, category)

        if not include_cfd:
            data = [elem for elem in data if 'CFD' not in elem['description']]

        data = [
            {'symbol':
                 re.sub('\..*$', '', elem['symbol']),
             'country':
                 re.sub('\_.*$', '', elem['symbol'].split('.')[1])
             }
            for elem in data
        ]

        if group_by_country:
            countries = set([elem['country'] for elem in data])
            data = {country: list(set([elem['symbol'] for elem in data if elem['country'] == country]))
                    for country in countries
                    }

        return data

    def get_names(self, category: str = None):

        data = self._raw_data if self._raw_data is not None else self._get_raw_symbols()

        if category is not None:
            data = self.__filter_categories(data, category)

        data = [elem for elem in data if 'CFD' not in elem['description']]

        data = [
            {'symbol':
                 re.sub('\..*$', '', elem['symbol']),
             'country':
                 re.sub('\_.*$', '', elem['symbol'].split('.')[1]),
             'name':
                 re.sub(r'\([^)]*\)', '', elem['description']).strip()
             }
            for elem in data
        ]
        return data

