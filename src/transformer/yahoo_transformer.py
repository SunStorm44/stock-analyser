# TODO Add function that would filter certain dates based on what's already in the internal DB
from typing import Union
import pandas as pd
from datetime import date

from src.transformer.data_transformer import DataTransformer
from src.loader.db_loader import DbLoader
from src.extractor.db_extractor import DbExtractor
from src.database.models import balance_sheet, cashflow_statement, profit_loss_statement, stock_statistics, stock_profile
from src.utils.exceptions import EmptyStatementError


class YahooTransformer(DataTransformer):
    def __init__(self,
                 raw_data: Union[dict, None] = None,
                 blnc_sht: Union[dict, None] = None,
                 inc_stmt: Union[dict, None] = None,
                 cash_stmt: Union[dict, None] = None,
                 comp_stats: Union[dict, None] = None,
                 comp_profile: Union[dict, None] = None
                 ):
        super().__init__()
        self._balance_sheet = raw_data['balance'] if raw_data else blnc_sht
        self._income_statement = raw_data['income'] if raw_data else inc_stmt
        self._cashflow_statement = raw_data['cash'] if raw_data else cash_stmt
        self._company_statistics = raw_data['stats'] if raw_data else comp_stats
        self._company_profile = raw_data['profile'] if raw_data else comp_profile

        self._db_extr = DbExtractor(auto_login=True)
        self._db_load = DbLoader(auto_login=True)

    @property
    def raw_data(self):
        return self._raw_data

    @raw_data.setter
    def raw_data(self, value):
        self._raw_data = value

    @property
    def balance_sheet(self):
        return self._balance_sheet

    @balance_sheet.setter
    def balance_sheet(self, value):
        self._balance_sheet = value

    @property
    def income_statement(self):
        return self._income_statement

    @income_statement.setter
    def income_statement(self, value):
        self._income_statement = value

    @property
    def cashflow_statement(self):
        return self._cashflow_statement

    @cashflow_statement.setter
    def cashflow_statement(self, value):
        self._cashflow_statement = value

    @property
    def company_statistics(self):
        return self._company_statistics

    @company_statistics.setter
    def company_statistics(self, value):
        self._company_statistics = value

    @property
    def company_profile(self):
        return self._company_profile

    @company_profile.setter
    def company_profile(self, value):
        self._company_profile = value

    def __handle_empty_error(self, symbol, error) -> None:
        comp, country = symbol.split('-')
        err_ticks = self._db_extr.get_erroneous()
        if comp not in err_ticks:
            self._db_load.load_erroneous(comp, country, error)
        print(f'Could not process data for {comp}-{country}. Reason: {error}')

    def _filter_columns(self, statement_type) -> pd.DataFrame:
        if statement_type == 'balance':
            cols = [col.name for col in balance_sheet.BalanceSheet.__table__.columns]
        elif statement_type == 'income':
            cols = [col.name for col in profit_loss_statement.ProfitLossStmt.__table__.columns]
        elif statement_type == 'cash':
            cols = [col.name for col in cashflow_statement.CashFlowStmt.__table__.columns]
        elif statement_type == 'stats':
            cols = [col.name for col in stock_statistics.StockStatistics.__table__.columns]
        elif statement_type == 'profile':
            cols = [col.name for col in stock_profile.StockProfile.__table__.columns]
        else:
            raise ValueError(f'{statement_type} was not recognized. Please correct and run again.')

        if not set(cols).issubset(set(self._processed_data.columns)):
            self._processed_data = self._processed_data.reindex(columns=cols, fill_value=None)

        self._processed_data = self._processed_data[cols]
        return self._processed_data

    def process_data(self,
                     blnc_sht: dict = None,
                     inc_stmt: dict = None,
                     cash_stmt: dict = None,
                     comp_stats: dict = None,
                     comp_profile: dict = None
                     ) -> dict:

        self._balance_sheet = blnc_sht if blnc_sht is not None else self._balance_sheet
        self._income_statement = inc_stmt if inc_stmt is not None else self._income_statement
        self._cashflow_statement = cash_stmt if cash_stmt is not None else self._cashflow_statement
        self._company_statistics = comp_stats if comp_stats is not None else self._company_statistics
        self._company_profile = comp_profile if comp_profile is not None else self._company_profile

        results = {'balance': self.process_statement(self._balance_sheet, 'balance'),
                   'income': self.process_statement(self._income_statement, 'income'),
                   'cash': self.process_statement(self._cashflow_statement, 'cash'),
                   'stats': self.process_company_data(self._company_statistics, 'stats'),
                   'profile': self.process_company_data(self._company_profile, 'profile'),
                   }

        return results

    def process_statement(self,
                          raw_data: dict = None,
                          statement_type: str = None) -> pd.DataFrame:
        self._raw_data = raw_data if raw_data else self._raw_data

        tickers = list(self._raw_data.keys())
        full_data = []

        for tick in tickers:
            data = self._raw_data[tick]
            if not data:
                self.__handle_empty_error(tick, f"Empty {statement_type}")
                continue
            for elem in data:
                df = pd.DataFrame.from_dict(elem, orient='index')
                df.reset_index(names='asof_date', inplace=True)
                df[['ticker', 'country']] = tick.split('-')
                full_data.append(df)
        if not full_data:
            raise EmptyStatementError
        self._processed_data = pd.concat(full_data)
        self._processed_data = self._filter_columns(statement_type)
        return self._processed_data

    def process_company_data(self,
                             raw_data: dict = None,
                             data_type: str = None):
        self._raw_data = raw_data if raw_data else self._raw_data

        tickers = list(self._raw_data.keys())
        full_data = []

        for tick in tickers:
            data = self._raw_data[tick]
            df = pd.DataFrame(data)
            df['asof_date'] = date.today()
            df[['ticker', 'country']] = tick.split('-')
            full_data.append(df)
        self._processed_data = pd.concat(full_data)
        self._processed_data = self._filter_columns(data_type)
        return self._processed_data

