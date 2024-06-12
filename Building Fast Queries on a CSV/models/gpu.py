from sqlalchemy import Column, Integer, String
from .base import Base

class GPU(Base):
    __tablename__ = 'gpu'
    gpu_id = Column(Integer, primary_key=True, autoincrement=True)
    gpu_brand = Column(String, nullable=False)
    gpu_model = Column(String, unique=False, nullable=False)