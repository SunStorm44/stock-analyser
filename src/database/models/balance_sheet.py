from sqlalchemy import Column, BigInteger, Date, String
from ..database import Base, BS_TABLENAME


class BalanceSheet(Base):
    __tablename__ = BS_TABLENAME

    asof_date = Column(Date, primary_key=True, index=True, nullable=False)
    ticker = Column(String, primary_key=True, index=True, nullable=False)
    country = Column(String, primary_key=True, nullable=False)
    totalAssets = Column(BigInteger)
    longTermDebt = Column(BigInteger)
    currentAssets = Column(BigInteger)
    currentLiabilities = Column(BigInteger)
    commonStock = Column(BigInteger)
