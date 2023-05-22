from sqlalchemy import Column, String, BOOLEAN
from ..database import Base, ERRONEOUS_SYMBOLS_TABLENAME


class ErroneousSymbols(Base):
    __tablename__ = ERRONEOUS_SYMBOLS_TABLENAME

    ticker = Column(String, primary_key=True)
    suffix = Column(String, primary_key=True)
    country = Column(String)
    reason = Column(String)
    confirmed_manually = Column(BOOLEAN)

