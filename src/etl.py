# TODO Change country suffixes to accept list of values

import time

from src.extractor.db_extractor import DbExtractor
from src.extractor.xtb_extractor import XtbExtractor
from src.extractor.yahoo_extractor import YahooExtractor
from src.transformer.yahoo_transformer import YahooTransformer
from src.transformer.xtb_transformer import XtbTransformer
from src.loader.db_loader import DbLoader

from src.utils.auxiliary import grouper, process_symbols
from src.utils.exceptions import EmptyStatementError


def yahoo_etl():
    """Run ETL on YAHOO data"""
    db_extractor = DbExtractor(auto_login=True)
    symbols = db_extractor.get_table_contents('sa_country_mapping')
    symbols = process_symbols(symbols)

    # symbols = {'DE': ['NVD']}

    # Due to the amount of companies, run the ETL in chunks
    for ticker, companies in symbols.items():
        for chunk in grouper(companies, 5):
            chunk = [x for x in chunk if x is not None]
            print(f'Processing {ticker} - {chunk} please wait...')
            d = {ticker: chunk}

            # Extract
            ye = YahooExtractor(grouped_symbols=d)
            yh_data = ye.get_data()

            if all(bool(d) for d in yh_data.values()):
                try:
                    # Transform
                    yt = YahooTransformer(yh_data)
                    yh_transformed = yt.process_data()
                except EmptyStatementError:
                    continue
                # Load
                db_loader = DbLoader(auto_login=True)
                db_loader.load_statements(yh_transformed)
                print(f'{ticker} - {chunk} have been processed by the ETL...')
                time.sleep(7)  # To avoid sending too many requests to Yahoo API in a short period of time


def xtb_etl():
    """Run ETL on XTB data"""
    # Extract
    xtb = XtbExtractor(auto_login=True)
    names = xtb.get_names(category='STC')

    # Transform
    xt = XtbTransformer(names)
    names_df = xt.process_data()

    # Load
    loader = DbLoader(True)
    loader.load_country_mapping(names_df)



