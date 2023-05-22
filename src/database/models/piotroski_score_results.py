from sqlalchemy import Column, Float, Date, String, BigInteger, Integer
from ..database import Base, PIOTROSKI_RESULTS_TABLENAME


class PiotroskiFScore(Base):
    __tablename__ = PIOTROSKI_RESULTS_TABLENAME

    ticker = Column(String, primary_key=True, index=True, nullable=False)
    country = Column(String, primary_key=True, nullable=False)
    name = Column(String)
    piotroskiFScore = Column(Integer)
    trailingPE = Column(Float)
    lastAsofDate = Column(Date)
    industry = Column(String)
    sector = Column(String)
    currency = Column(String)
    dividendRate = Column(Float)
    volume = Column(Float)
    marketCap = Column(BigInteger)
    twoHundredDayAverage = Column(Float)
    website = Column(String)
    longBusinessSummary = Column(String)
