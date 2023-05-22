# TODO Stop using filter_existing_tickers when Yahoo Transformer would be updated
from typing import Union
from yahoofinancials import YahooFinancials
import re

from src.config.variables import *
from src.extractor.data_extractor import DataExtractor
from src.extractor.db_extractor import DbExtractor
from src.loader.db_loader import DbLoader


class YahooExtractor(DataExtractor):
    def __init__(self, grouped_symbols: dict):
        """
        YahooExtractor class constructor.

        :param grouped_symbols: Python dict containing all symbols necessary to be queried from Yahoo Finance.
        It should have the following structure: {country_code: ['symbol 1', 'symbol 2', etc.]}

        Example:
        grouped_symbols = {'US': ['AAPL', 'GOOG'], 'CH': ['CFR']}
        """
        super().__init__()

        self._grouped_symbols: dict = grouped_symbols
        self._yf: Union[YahooFinancials, None] = None
        self._db_extr = DbExtractor(auto_login=True)
        self._db_load = DbLoader(auto_login=True)
        self._erroneous_tickers: list = self._db_extr.get_erroneous()

    @property
    def grouped_symbols(self):
        return self._grouped_symbols

    @grouped_symbols.setter
    def grouped_symbols(self, value):
        self._grouped_symbols = value

    @property
    def erroneous_tickers(self):
        return self._erroneous_tickers

    def connect(self):
        pass

    def __handle_error(self, company, country, error) -> None:
        comp = re.sub('\..*$', '', company)
        if comp not in self._erroneous_tickers:
            self._db_load.load_erroneous(comp, country, error)
            self._erroneous_tickers = self._db_extr.get_erroneous()
        print(f'Could not get data for {comp}-{country}. Reason: {error}...')

    def get_data(self):
        self.filter_tickers()
        self.preprocess_tickers()
        results: dict = {'balance': self.get_financial_data('balance'),
                         'income': self.get_financial_data('income'),
                         'cash': self.get_financial_data('cash'),
                         'stats': self.get_company_statistics('stats'),
                         'profile': self.get_company_statistics('profile'),
                         }
        return results

    def preprocess_tickers(self):
        """Pre-processes tickers to make the searchable in Yahoo Finance"""
        for country, companies in self._grouped_symbols.items():
            if country not in TICKER_SUFFIXES.keys():
                continue
            new_comps = [comp + f'.{TICKER_SUFFIXES[country]}' for comp in companies]
            self._grouped_symbols[country] = new_comps

    def filter_tickers(self):
        """Filters out all tickers that are already present in the statement tables"""
        existing_tickers = self._db_extr.get_all_tickers()

        for country, companies in self._grouped_symbols.items():
            comps = [comp for comp in companies if comp not in existing_tickers]  # Filter existing
            comps = [comp for comp in comps if comp not in self._erroneous_tickers] # Filter erroneous
            self.grouped_symbols[country] = comps

    # noinspection PyMethodOverriding
    def get_financial_data(self,
                           statement_type: str,
                           frequency: str = 'annual') -> dict:
        """
        Financial statement data extraction method.

        :param statement_type: Type of data to extract. The following values are accepted:
            - 'balance' for Balance Sheet
            - 'income' for Income Statement
            - 'cash' for Cashflow Statement
        :param frequency: Frequency of the financial statement data.
        The following values are accepted:
            - 'annual' for yearly data
            - 'quarterly' for quarterly data
        :return: raw Yahoo Finance data formatted as Python dictionary.
        """
        def pre_process_statement(statement, country, company) -> dict:
            comp = re.sub('\..*$', '', company)
            stmt = {comp: v for k, v in list(statement.values())[0].items()}
            stmt[f'{comp}-{country}'] = stmt.pop(comp)
            return stmt

        results = dict()
        for country, companies in self._grouped_symbols.items():
            for comp in companies:
                try:
                    self._yf = YahooFinancials(comp)
                    stmt = self._yf.get_financial_stmts(frequency, statement_type)
                except Exception as e:
                    self.__handle_error(comp, country, str(e))
                    continue
                else:
                    results.update(pre_process_statement(stmt, country, comp))
        return results

    def get_company_statistics(self, statement_type: str) -> dict:
        def pre_process_stats(statement, country, company) -> dict:
            comp = re.sub('\..*$', '', company)
            stmt = {comp: [v] for k, v in statement.items()}
            stmt[f'{comp}-{country}'] = stmt.pop(comp)
            return stmt

        results = dict()
        for country, companies in self._grouped_symbols.items():
            for comp in companies:
                try:
                    self._yf = YahooFinancials(comp)
                    if statement_type == 'stats':
                        stmt = self._yf.get_summary_data()
                    elif statement_type == 'profile':
                        stmt = self._yf.get_stock_profile_data()
                    else:
                        raise ValueError(f"Unrecognized statement type. 'stats' or 'profile' accepted. "
                                         f"{statement_type} passed")
                except Exception as e:
                    self.__handle_error(comp, country, str(e))
                    continue
                else:
                    results.update(pre_process_stats(stmt, country, comp))
            return results
