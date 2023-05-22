from sqlalchemy import Column, String
from ..database import Base, STOCK_PROFILE_TABLENAME


class StockProfile(Base):
    __tablename__ = STOCK_PROFILE_TABLENAME

    ticker = Column(String, primary_key=True, index=True, nullable=False)
    country = Column(String, primary_key=True, nullable=False)
    industry = Column(String)
    sector = Column(String)
    website = Column(String)
    longBusinessSummary = Column(String)
