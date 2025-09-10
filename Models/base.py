#!/usr/bin/env python3
"""
Modelo base para SQLAlchemy
===========================

Define la clase base con funcionalidades comunes para todos los modelos.

Autor: Miguel Bonilla
Fecha: Septiembre 2025
"""

from sqlalchemy import Column, Integer, DateTime, func
from sqlalchemy.ext.declarative import declared_attr, declarative_base
import datetime

# Base declarativa - se define aquí para evitar imports circulares
Base = declarative_base()

class BaseModel(Base):
    """
    Clase base abstracta para todos los modelos.
    Incluye campos comunes como timestamps.
    """
    __abstract__ = True
    
    # ID autoincremental para todas las tablas
    id = Column(Integer, primary_key=True, autoincrement=True)
    
    # Timestamps automáticos
    created_at = Column(DateTime, default=func.now(), nullable=False)
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now(), nullable=False)
    
    @declared_attr
    def __tablename__(cls):
        """Genera automáticamente el nombre de tabla en snake_case."""
        import re
        name = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', cls.__name__)
        return re.sub('([a-z0-9])([A-Z])', r'\1_\2', name).lower()
    
    def to_dict(self):
        """Convierte el modelo a diccionario."""
        return {
            column.name: getattr(self, column.name)
            for column in self.__table__.columns
        }
    
    def __repr__(self):
        """Representación string del modelo."""
        class_name = self.__class__.__name__
        return f"<{class_name}(id={getattr(self, 'id', None)})>"
