#!/usr/bin/env python3
"""
Modelo Constructor (Equipo)
===========================

Define la estructura de datos para los constructores/equipos de Fórmula 1.

Autor: Miguel Bonilla
Fecha: Septiembre 2025
"""

from sqlalchemy import Column, String, Index
from sqlalchemy.orm import relationship
from Models.base import BaseModel

class Constructor(BaseModel):
    """
    Modelo para los constructores/equipos de Fórmula 1.
    """
    __tablename__ = 'constructors'
    
    # Campos principales
    constructor_id = Column(String(50), unique=True, nullable=False, index=True)
    name = Column(String(100), nullable=False)
    nationality = Column(String(50), nullable=True)
    
    # Relaciones
    qualifying_results = relationship("QualifyingResult", back_populates="constructor")
    
    # Índices para mejorar rendimiento
    __table_args__ = (
        Index('idx_constructor_name', 'name'),
        Index('idx_constructor_nationality', 'nationality'),
    )
    
    def __repr__(self):
        return f"<Constructor(id={self.id}, name='{self.name}', nationality='{self.nationality}')>"
    
    @classmethod
    def get_or_create(cls, session, constructor_id, **kwargs):
        """
        Obtiene un constructor existente o crea uno nuevo.
        """
        constructor = session.query(cls).filter_by(constructor_id=constructor_id).first()
        
        if constructor is None:
            constructor = cls(constructor_id=constructor_id, **kwargs)
            session.add(constructor)
            
        return constructor
