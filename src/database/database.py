from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.engine import Engine
from sqlalchemy.orm.decl_api import DeclarativeMeta

from src.config import CREDENTIALS

BS_TABLENAME = 'sa_balance_sheet'
PNL_TABLENAME = 'sa_profit_loss_statement'
CF_TABLENAME = 'sa_cashflow_statement'
STATS_TABLENAME = 'sa_stock_statistics'
COUNTRY_MAPPING_TABLENAME = 'sa_country_mapping'
STOCK_PROFILE_TABLENAME = 'sa_stock_profile'
PIOTROSKI_RESULTS_TABLENAME = 'sa_piotroski_results'
ERRONEOUS_SYMBOLS_TABLENAME = 'sa_erroneous_symbols'

db_url: str = CREDENTIALS['stock_analyser_database_url']

engine: Engine = create_engine(
    db_url, connect_args={'check_same_thread': False}, echo=True
)

SessionLocal: sessionmaker = sessionmaker(autocommit=False, autoflush=False, bind=engine)

Base: DeclarativeMeta = declarative_base()
