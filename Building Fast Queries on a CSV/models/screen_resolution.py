from sqlalchemy import Column, Integer, String
from .base import Base

class ScreenResolution(Base):
    __tablename__ = 'screen_resolution'
    screen_id = Column(Integer, primary_key=True, autoincrement=True)
    screen_size = Column(String, nullable=False)
    screen_type = Column(String, nullable=False)