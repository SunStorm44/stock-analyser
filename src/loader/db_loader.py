from sqlalchemy import MetaData
import pandas as pd

from src.database.database import engine
from src.database.models import balance_sheet, profit_loss_statement, cashflow_statement, \
    stock_statistics, country_mapping, stock_profile, erroneous_symbols


class DbLoader:
    def __init__(self, auto_login=False):
        self._db_engine = engine
        self._conn = None
        self._balance_tbl = balance_sheet.BalanceSheet
        self._income_tbl = profit_loss_statement.ProfitLossStmt
        self._cashflow_tbl = cashflow_statement.CashFlowStmt
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

    def _get_tablename(self, name: str):
        if name == 'balance':
            return self._balance_tbl.__tablename__
        elif name == 'income':
            return self._income_tbl.__tablename__
        elif name == 'cash':
            return self._cashflow_tbl.__tablename__
        elif name == 'stats':
            return self._stock_stats.__tablename__
        elif name == 'profile':
            return self._stock_profile.__tablename__

    def append_data(self, name: str, data: pd.DataFrame) -> None:
        data.to_sql(name,
                    self._db_engine,
                    if_exists='append',
                    index=False)

    def load_erroneous(self, ticker: str, country: str, error: str) -> None:
        name = self._erroneous_symbols.__tablename__
        data = [
            {'ticker': ticker, 'country': country, 'reason': error}
        ]
        df = pd.DataFrame(data)
        self.append_data(name, df)

    def delete_contents(self, tablename: str):
        names = self._db_engine.table_names()
        if tablename not in names:
            raise ValueError(f'{tablename} was not found in the database.')

        tbl = [tbl for tbl in list(self._metadata.tables.values()) if tbl.fullname == tablename][0]
        delete_stmt = tbl.delete()
        self._conn.execute(delete_stmt)

    def load_country_mapping(self, mapping: pd.DataFrame):
        name = self._country_mapping.__tablename__

        self.delete_contents(name)
        mapping.to_sql(name,
                       self._db_engine,
                       if_exists='append',
                       index=False)

    def load_statements(self, statements: dict):
        for stmt_type, data in statements.items():
            name = self._get_tablename(stmt_type)
            self.append_data(name, data)
