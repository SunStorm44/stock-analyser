from sqlalchemy import select, MetaData
from sqlalchemy.sql import and_
import pandas as pd
from typing import Union

from .data_extractor import DataExtractor
from src.database.database import engine
from src.database.models import balance_sheet, profit_loss_statement, \
    cashflow_statement, stock_statistics, country_mapping, stock_profile, erroneous_symbols


class DbExtractor(DataExtractor):
    def __init__(self, auto_login=False):
        super().__init__()

        self._db_engine = engine
        self._conn = None
        self._balance_tbl = balance_sheet.BalanceSheet
        self._income_tbl = profit_loss_statement.ProfitLossStmt
        self._cashflow_table = cashflow_statement.CashFlowStmt
        self._stock_stats = stock_statistics.StockStatistics
        self._stock_profile = stock_profile.StockProfile
        self._country_mapping = country_mapping.CountryMapping
        self._erroneous_symbols = erroneous_symbols.ErroneousSymbols

        self._metadata = MetaData()
        self._metadata.reflect(bind=self._db_engine)

        if auto_login:
            self.connect()

    def connect(self):
        self._conn = engine.connect()

    def disconnect(self):
        self._conn.close()
        self._db_engine.dispose()

    def get_data(self):
        pass

    def get_erroneous(self):
        df = self.get_table_contents('sa_erroneous_symbols')
        return df['ticker'].values.tolist()

    def get_all_tickers(self, output_type: str = 'ticker') -> list:
        query = select([self._balance_tbl.ticker, self._balance_tbl.country]). \
            where(and_(self._balance_tbl.ticker.in_(select([self._income_tbl.ticker], from_obj=self._income_tbl)),
                       self._balance_tbl.ticker.in_(select([self._cashflow_table.ticker], from_obj=self._cashflow_table)),
                       self._balance_tbl.ticker.in_(select([self._stock_stats.ticker], from_obj=self._stock_stats)),
                       self._balance_tbl.ticker.in_(select([self._stock_profile.ticker], from_obj=self._stock_profile))))
        if output_type == 'ticker':
            return list(set([tick[0] for tick in self._conn.execute(query)]))
        elif output_type == 'symbol':
            return list(set([f'{tick[0]}-{tick[1]}' for tick in self._conn.execute(query)]))
        else:
            raise ValueError(f"Wrong output_type. 'ticker' or 'symbol' accepted. {output_type} passed.")

    def get_all_symbols(self) -> list:
        query = select([self._balance_tbl.ticker, self._balance_tbl.country]). \
            where(and_(self._balance_tbl.ticker.in_(select([self._income_tbl.ticker], from_obj=self._income_tbl)),
                       self._balance_tbl.ticker.in_(select([self._cashflow_table.ticker], from_obj=self._cashflow_table)),
                       self._balance_tbl.ticker.in_(select([self._stock_stats.ticker], from_obj=self._stock_stats)),
                       self._balance_tbl.ticker.in_(select([self._stock_profile.ticker], from_obj=self._stock_profile))))
        result = list(set([tick[0] for tick in self._conn.execute(query)]))
        return result

    def get_table_contents(self,
                           tablename: str,
                           filter_kwargs: Union[dict, None] = None) -> pd.DataFrame:
        names = self._db_engine.table_names()
        if tablename not in names:
            raise ValueError(f'{tablename} was not found in the database.')

        tbl = [tbl for tbl in list(self._metadata.tables.values()) if tbl.fullname == tablename][0]

        if filter_kwargs is not None:
            query = tbl.select().filter_by(**filter_kwargs)
        else:
            query = tbl.select()

        result = self._conn.execute(query).fetchall()
        df = pd.DataFrame(result, columns=tbl.columns.keys())
        return df
