from sqlalchemy import Column, String
from ..database import Base, COUNTRY_MAPPING_TABLENAME


class CountryMapping(Base):
    __tablename__ = COUNTRY_MAPPING_TABLENAME

    ticker = Column(String, primary_key=True, index=True, nullable=False)
    country = Column(String, primary_key=True, nullable=False)
    name = Column(String)

