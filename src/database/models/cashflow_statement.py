from sqlalchemy import Column, BigInteger, Date, String
from ..database import Base, CF_TABLENAME


class CashFlowStmt(Base):
    __tablename__ = CF_TABLENAME

    asof_date = Column(Date, primary_key=True, index=True, nullable=False)
    ticker = Column(String, primary_key=True, index=True, nullable=False)
    country = Column(String, primary_key=True, nullable=False)
    operatingCashFlow = Column(BigInteger)
