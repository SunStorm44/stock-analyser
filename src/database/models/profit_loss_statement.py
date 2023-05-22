from sqlalchemy import Column, BigInteger, Date, String
from ..database import Base, PNL_TABLENAME


class ProfitLossStmt(Base):
    __tablename__ = PNL_TABLENAME

    asof_date = Column(Date, primary_key=True, index=True, nullable=False)
    ticker = Column(String, primary_key=True, index=True, nullable=False)
    country = Column(String, primary_key=True, nullable=False)
    netIncome = Column(BigInteger)
    totalRevenue = Column(BigInteger)
    grossProfit = Column(BigInteger)
