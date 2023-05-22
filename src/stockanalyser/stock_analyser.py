from typing import Union
import pandas as pd

from src.database.models import balance_sheet, cashflow_statement, profit_loss_statement, \
    stock_statistics, stock_profile, country_mapping, piotroski_score_results
from src.extractor.db_extractor import DbExtractor
from src.loader.db_loader import DbLoader
from src.config.variables import *


class StockAnalyser:
    def __init__(self, symbols: Union[str, list, None] = None):
        self._symbols = symbols
        self._db_extr = DbExtractor(auto_login=True)
        self._db_load = DbLoader(auto_login=True)

    @property
    def symbols(self):
        return self._symbols

    @symbols.setter
    def symbols(self, value):
        self._symbols = value

    def merge_financial_data(self, symbol: str):
        """Merge all gathered inputs for the passed ticker"""
        ticker, country = symbol.split('-')

        blnc_sht = self._db_extr.get_table_contents(balance_sheet.BalanceSheet.__tablename__, {'ticker': ticker,
                                                                                               'country': country})
        prft_stmt = self._db_extr.get_table_contents(profit_loss_statement.ProfitLossStmt.__tablename__, {'ticker': ticker,
                                                                                                          'country': country})
        cash_stmt = self._db_extr.get_table_contents(cashflow_statement.CashFlowStmt.__tablename__, {'ticker': ticker,
                                                                                                     'country': country})

        df = blnc_sht.merge(prft_stmt, on='asof_date', how='inner', suffixes=('', '_drop')) \
            .merge(cash_stmt, on='asof_date', how='inner', suffixes=('', '_drop'))

        df.drop(list(df.filter(regex='_drop$')), axis=1, inplace=True)
        df.sort_values('asof_date', ascending=False, inplace=True, ignore_index=True)

        return df

    def prepare_output(self,
                       symbol: str,
                       indicators: Union[dict, None] = None,
                       sort_by: Union[dict, None] = None,
                       output_columns: Union[list, None] = None):
        ticker, country = symbol.split('-')

        stats = self._db_extr.get_table_contents(stock_statistics.StockStatistics.__tablename__, {'ticker': ticker,
                                                                                                  'country': country})
        profile = self._db_extr.get_table_contents(stock_profile.StockProfile.__tablename__, {'ticker': ticker,
                                                                                              'country': country})
        names = self._db_extr.get_table_contents(country_mapping.CountryMapping.__tablename__, {'ticker': ticker,
                                                                                                'country': country})

        df = stats.merge(profile, on='ticker', how='inner', suffixes=('', '_drop'))\
            .merge(names, on='ticker', how='inner', suffixes=('', '_drop'))

        df.drop(list(df.filter(regex='_drop$')), axis=1, inplace=True)

        if indicators is not None:
            df = df.assign(**indicators)

        if sort_by is not None:
            cols = list(sort_by.keys())
            directions = [True if elem == 'ascending' else False for elem in list(sort_by.values())]
            df.sort_values(cols, ascending=directions, inplace=True, na_position='last')

        if output_columns is not None:
            df = df[output_columns]

        return df

    @staticmethod
    def _calc_piotroski_score(df: pd.DataFrame) -> Union[int, str]:

        # Assessing appropriate year values for further calculation
        try:
            ly = df.asof_date[0]
            py = df.asof_date[1]
            ppy = df.asof_date[2]
        except KeyError:
            return 'No Data'

        # Obtaining specific metrics from the dataframe to make the Piotroski f-score calculation more clear
        ni_ly = df[df['asof_date'] == ly]['netIncome'].values.item()
        ni_py = df[df['asof_date'] == py]['netIncome'].values.item()

        ta_ly = df[df['asof_date'] == ly]['totalAssets'].values.item()
        ta_py = df[df['asof_date'] == py]['totalAssets'].values.item()
        ta_ppy = df[df['asof_date'] == ppy]['totalAssets'].values.item()

        cfo_ly = df[df['asof_date'] == ly]['operatingCashFlow'].values.item()

        ltd_ly = df[df['asof_date'] == ly]['longTermDebt'].values.item()
        ltd_py = df[df['asof_date'] == py]['longTermDebt'].values.item()

        cass_ly = df[df['asof_date'] == ly]['currentAssets'].values.item()
        cass_py = df[df['asof_date'] == py]['currentAssets'].values.item()

        clia_ly = df[df['asof_date'] == ly]['currentLiabilities'].values.item()
        clia_py = df[df['asof_date'] == py]['currentLiabilities'].values.item()

        coms_ly = df[df['asof_date'] == ly]['commonStock'].values.item()
        coms_py = df[df['asof_date'] == py]['commonStock'].values.item()

        rev_ly = df[df['asof_date'] == ly]['totalRevenue'].values.item()
        rev_py = df[df['asof_date'] == py]['totalRevenue'].values.item()

        gprof_ly = df[df['asof_date'] == ly]['grossProfit'].values.item()
        gprof_py = df[df['asof_date'] == py]['grossProfit'].values.item()

        # Calculating actual Piotroski F-Score metrics
        try:
            ROA_FS = int(ni_ly / ((ta_ly + ta_py) / 2) > 0)
        except (TypeError, ZeroDivisionError):
            ROA_FS = 0

        try:
            CFO_FS = int(cfo_ly > 0)
        except (TypeError, ZeroDivisionError):
            CFO_FS = 0

        try:
            ROA_D_FS = int(ni_ly / ((ta_ly + ta_py) / 2) > ni_py / ((ta_py + ta_ppy) / 2))
        except (TypeError, ZeroDivisionError):
            ROA_D_FS = 0

        try:
            CFO_ROA_FS = int(cfo_ly / ta_ly > ni_ly / ((ta_ly + ta_py) / 2))
        except (TypeError, ZeroDivisionError):
            CFO_ROA_FS = 0

        try:
            LTD_FS = int(ltd_ly / ta_ly < ltd_py / ta_py)
        except (TypeError, ZeroDivisionError):
            LTD_FS = 0

        try:
            CR_FS = int(cass_ly / clia_ly > cass_py / clia_py)
        except (TypeError, ZeroDivisionError):
            CR_FS = 0

        try:
            DILUTION_FS = int(coms_ly <= coms_py)
        except (TypeError, ZeroDivisionError):
            DILUTION_FS = 0

        try:
            GM_FS = int(gprof_ly / rev_ly > gprof_py / rev_py)
        except (TypeError, ZeroDivisionError):
            GM_FS = 0

        try:
            ATO_FS = int(rev_ly / ((ta_ly + ta_py) / 2) > rev_py / ((ta_py + ta_ppy) / 2))
        except (TypeError, ZeroDivisionError):
            ATO_FS = 0

        results = [ROA_FS, CFO_FS, ROA_D_FS, CFO_ROA_FS, LTD_FS, CR_FS, DILUTION_FS, GM_FS, ATO_FS]
        results_sum = sum(results)

        return results_sum

    def piotroski_f_score(self, symbols: Union[str, list, None] = None) -> pd.DataFrame:
        self._symbols = symbols if symbols is not None else self._symbols

        if isinstance(self._symbols, str):
            self._symbols = [self._symbols]

        out = list()
        for comp in self._symbols:
            df = self.merge_financial_data(comp)
            score = self._calc_piotroski_score(df)
            ly = df.asof_date[0]
            out.append(self.prepare_output(comp,
                                           indicators={'piotroskiFScore': score, 'lastAsofDate': ly},
                                           sort_by={'piotroskiFScore': 'descending', 'trailingPE': 'ascending'},
                                           output_columns=PIOTROSKI_OUT_COLS))
        out_df = pd.concat(out)

        pfscore_name = piotroski_score_results.PiotroskiFScore.__tablename__
        self._db_load.delete_contents(pfscore_name)
        self._db_load.append_data(pfscore_name, out_df)

        return out_df
