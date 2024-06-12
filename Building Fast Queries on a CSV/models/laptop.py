from sqlalchemy import Column, Integer, String, Float, ForeignKey, create_engine
from sqlalchemy.orm import relationship

from .base import Base

class Laptop(Base):
    __tablename__ = 'laptop'
    
    # Primary Key
    laptop_id = Column(Integer, primary_key=True, autoincrement=True)
    
    # Foreign Keys
    cpu_id = Column(Integer, ForeignKey('cpu.cpu_id'), nullable=False)
    gpu_id = Column(Integer, ForeignKey('gpu.gpu_id'), nullable=False)
    screen_id = Column(Integer, ForeignKey('screen_resolution.screen_id'), nullable=False)
    
    # Other Columns
    company = Column(String, nullable=False)
    product = Column(String, nullable=False)
    type_name = Column(String, nullable=False)
    ram = Column(String, nullable=False)
    memory = Column(String, nullable=False)
    inches = Column(Float, nullable=False)
    opsys = Column(String, nullable=False)
    weight_kg = Column(Float, nullable=False)
    price = Column(Float, nullable=False)
    
    # Relationships
    screen_resolution = relationship('ScreenResolution')
    cpu = relationship('CPU')
    gpu = relationship('GPU')