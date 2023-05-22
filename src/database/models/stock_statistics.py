from sqlalchemy import Column, Float, Date, String, BigInteger
from ..database import Base, STATS_TABLENAME


class StockStatistics(Base):
    __tablename__ = STATS_TABLENAME

    asof_date = Column(Date, primary_key=True, index=True, nullable=False)
    ticker = Column(String, primary_key=True, index=True, nullable=False)
    country = Column(String, primary_key=True, nullable=False)
    currency = Column(String)
    trailingPE = Column(Float)
    dividendRate = Column(Float)
    volume = Column(Float)
    marketCap = Column(BigInteger)
    twoHundredDayAverage = Column(Float)
