#!/usr/bin/env python3
"""
Modelo QualifyingResult
=======================

Define la estructura de datos para los resultados de qualifying de Fórmula 1.

Autor: Miguel Bonilla
Fecha: Septiembre 2025
"""

from sqlalchemy import Column, String, Integer, ForeignKey, Index, UniqueConstraint
from sqlalchemy.orm import relationship
from Models.base import BaseModel

class QualifyingResult(BaseModel):
    """
    Modelo para los resultados de qualifying de Fórmula 1.
    """
    __tablename__ = 'qualifying_results'
    
    # Campos principales de la carrera
    season = Column(Integer, nullable=False, index=True)
    round = Column(Integer, nullable=False)
    circuitid = Column(String(50), nullable=False)  # Cambiado para coincidir con CSV
    position = Column(Integer, nullable=True)
    
    # Claves foráneas
    driver_db_id = Column(Integer, ForeignKey('drivers.id'), nullable=False)
    constructor_db_id = Column(Integer, ForeignKey('constructors.id'), nullable=False)
    
    # Tiempos de qualifying
    q1 = Column(String(20), nullable=True)  # Formato "mm:ss.sss" o "0"
    q2 = Column(String(20), nullable=True)
    q3 = Column(String(20), nullable=True)
    
    # Relaciones
    driver = relationship("Driver", back_populates="qualifying_results")
    constructor = relationship("Constructor", back_populates="qualifying_results")
    
    # Índices y restricciones
    __table_args__ = (
        # Índices compuestos para consultas frecuentes
        Index('idx_season_round', 'season', 'round'),
        Index('idx_season_driver', 'season', 'driver_db_id'),
        Index('idx_season_constructor', 'season', 'constructor_db_id'),
        Index('idx_circuit', 'circuitid'),  # Corregido el nombre del campo
        Index('idx_position', 'position'),
        
        # Restricción única: un piloto no puede tener múltiples resultados en la misma qualifying
        UniqueConstraint('season', 'round', 'driver_db_id', name='uq_qualifying_result'),
    )
    
    def __repr__(self):
        return (f"<QualifyingResult(id={self.id}, season={self.season}, "
                f"round={self.round}, position={self.position})>")
    
    @property
    def best_time(self):
        """Retorna el mejor tiempo de qualifying (Q1, Q2 o Q3)."""
        times = []
        
        for time_str in [self.q1, self.q2, self.q3]:
            if time_str and time_str != '0' and ':' in time_str:
                try:
                    # Convertir mm:ss.sss a segundos totales
                    parts = time_str.split(':')
                    if len(parts) == 2:
                        minutes = int(parts[0])
                        seconds = float(parts[1])
                        total_seconds = minutes * 60 + seconds
                        times.append((total_seconds, time_str))
                except (ValueError, IndexError):
                    continue
        
        if times:
            # Retornar el tiempo más rápido (menor número de segundos)
            return min(times, key=lambda x: x[0])[1]
        
        return None
    
    @property
    def has_valid_time(self):
        """Verifica si tiene al menos un tiempo válido de qualifying."""
        return self.best_time is not None
    
    def to_dict_extended(self):
        """Convierte a diccionario incluyendo información de driver y constructor."""
        base_dict = self.to_dict()
        
        if self.driver:
            base_dict.update({
                'driver_name': self.driver.full_name,
                'driver_code': self.driver.code,
                'driver_nationality': self.driver.nationality
            })
        
        if self.constructor:
            base_dict.update({
                'constructor_name': self.constructor.name,
                'constructor_nationality': self.constructor.nationality
            })
        
        base_dict['best_time'] = self.best_time
        
        return base_dict
