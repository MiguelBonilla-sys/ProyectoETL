#!/usr/bin/env python3
"""
Modelo Driver (Piloto)
======================

Define la estructura de datos para los pilotos de Fórmula 1.

Autor: Miguel Bonilla
Fecha: Septiembre 2025
"""

from sqlalchemy import Column, String, Date, Integer, Index
from sqlalchemy.orm import relationship
from Models.base import BaseModel

class Driver(BaseModel):
    """
    Modelo para los pilotos de Fórmula 1.
    """
    __tablename__ = 'drivers'
    
    # Campos principales
    driver_id = Column(String(50), unique=True, nullable=False, index=True)
    permanent_number = Column(Integer, nullable=True)
    code = Column(String(3), nullable=True, index=True)  # Código de 3 letras
    given_name = Column(String(100), nullable=False)
    family_name = Column(String(100), nullable=False)
    date_of_birth = Column(Date, nullable=True)
    nationality = Column(String(50), nullable=True)
    
    # Relaciones
    qualifying_results = relationship("QualifyingResult", back_populates="driver")
    
    # Índices compuestos para mejorar rendimiento
    __table_args__ = (
        Index('idx_driver_name', 'given_name', 'family_name'),
        Index('idx_driver_nationality', 'nationality'),
    )
    
    def __repr__(self):
        return f"<Driver(id={self.id}, code='{self.code}', name='{self.given_name} {self.family_name}')>"
    
    @property
    def full_name(self):
        """Retorna el nombre completo del piloto."""
        return f"{self.given_name} {self.family_name}"
    
    def generate_code(self):
        """Genera el código de 3 letras basado en el apellido."""
        if self.family_name:
            return self.family_name[:3].upper()
        return None
    
    @classmethod
    def get_or_create(cls, session, driver_id, **kwargs):
        """
        Obtiene un piloto existente o crea uno nuevo.
        """
        driver = session.query(cls).filter_by(driver_id=driver_id).first()
        
        if driver is None:
            # Generar código automáticamente si no se proporciona
            if 'code' not in kwargs and 'family_name' in kwargs:
                kwargs['code'] = kwargs['family_name'][:3].upper()
            
            driver = cls(driver_id=driver_id, **kwargs)
            session.add(driver)
            
        return driver
