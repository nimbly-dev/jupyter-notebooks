from sqlalchemy import Column, Integer, String
from .base import Base

class CPU(Base):
    __tablename__ = 'cpu'
    cpu_id = Column(Integer, primary_key=True, autoincrement=True)
    cpu_brand = Column(String, nullable=False)
    cpu_model = Column(String, unique=False, nullable=False)
    cpu_speed = Column(String, nullable=False)