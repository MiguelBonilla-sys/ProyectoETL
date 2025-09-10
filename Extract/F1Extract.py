#!/usr/bin/env python3
"""
Extractor de datos F1 - Versión 2.0 con SQLAlchemy
===================================================

Extrae datos tanto de archivos CSV como de bases de datos usando SQLAlchemy.

Autor: Miguel Bonilla
Fecha: Septiembre 2025
"""

import pandas as pd
from Database import get_session
from Models import QualifyingResult, Driver, Constructor
import logging

logger = logging.getLogger(__name__)

class Extractor:
    """
    Clase para extraer datos de archivos fuente y bases de datos.
    Ahora incluye capacidades SQLAlchemy para extraer desde BD.
    """
    def __init__(self, file_path=None):
        self.file_path = file_path

    def extract(self):
        """
        Extrae los datos del archivo especificado.
        """
        try:
            df = pd.read_csv(self.file_path)
            logger.info(f"✅ Datos extraídos de {self.file_path}: {len(df)} registros")
            return df
        except Exception as e:
            logger.error(f"❌ Error al extraer datos: {e}")
            return None

    def extract_csv(self, file_path):
        """
        Extrae datos de un archivo CSV específico.
        """
        try:
            df = pd.read_csv(file_path)
            logger.info(f"✅ Datos CSV extraídos de {file_path}: {len(df)} registros")
            return df
        except Exception as e:
            logger.error(f"❌ Error al extraer datos de {file_path}: {e}")
            return None
    
    def extract_from_database(self, season=None, limit=None):
        """
        Extrae datos de qualifying desde la base de datos usando SQLAlchemy.
        
        Args:
            season (int, optional): Filtrar por temporada específica
            limit (int, optional): Limitar número de resultados
            
        Returns:
            pandas.DataFrame: DataFrame con los resultados
        """
        try:
            with get_session() as session:
                # Construir query base con joins
                query = session.query(
                    QualifyingResult.season,
                    QualifyingResult.round,
                    QualifyingResult.circuit_id,
                    QualifyingResult.position,
                    Driver.driver_id,
                    Driver.code,
                    Driver.permanent_number,
                    Driver.given_name,
                    Driver.family_name,
                    Driver.date_of_birth,
                    Driver.nationality,
                    Constructor.constructor_id,
                    Constructor.name.label('constructor_name'),
                    Constructor.nationality.label('constructor_nationality'),
                    QualifyingResult.q1,
                    QualifyingResult.q2,
                    QualifyingResult.q3
                ).join(
                    Driver, QualifyingResult.driver_db_id == Driver.id
                ).join(
                    Constructor, QualifyingResult.constructor_db_id == Constructor.id
                )
                
                # Aplicar filtros opcionales
                if season:
                    query = query.filter(QualifyingResult.season == season)
                
                # Ordenar por temporada, ronda y posición
                query = query.order_by(
                    QualifyingResult.season.desc(),
                    QualifyingResult.round.desc(),
                    QualifyingResult.position.asc()
                )
                
                # Aplicar límite si se especifica
                if limit:
                    query = query.limit(limit)
                
                # Ejecutar query y convertir a DataFrame
                results = query.all()
                
                if not results:
                    logger.warning("⚠️ No se encontraron datos en la base de datos")
                    return pd.DataFrame()
                
                # Convertir a DataFrame
                df = pd.DataFrame([
                    {
                        'Season': result.season,
                        'Round': result.round,
                        'CircuitID': result.circuit_id,
                        'Position': result.position,
                        'DriverID': result.driver_id,
                        'Code': result.code,
                        'PermanentNumber': result.permanent_number,
                        'GivenName': result.given_name,
                        'FamilyName': result.family_name,
                        'DateOfBirth': result.date_of_birth,
                        'Nationality': result.nationality,
                        'ConstructorID': result.constructor_id,
                        'ConstructorName': result.constructor_name,
                        'ConstructorNationality': result.constructor_nationality,
                        'Q1': result.q1,
                        'Q2': result.q2,
                        'Q3': result.q3
                    }
                    for result in results
                ])
                
                logger.info(f"✅ Datos extraídos de BD: {len(df)} registros")
                return df
                
        except Exception as e:
            logger.error(f"❌ Error extrayendo datos de BD: {e}")
            return None
    
    def extract_drivers(self):
        """
        Extrae la lista de todos los pilotos desde la base de datos.
        
        Returns:
            pandas.DataFrame: DataFrame con información de pilotos
        """
        try:
            with get_session() as session:
                drivers = session.query(Driver).order_by(Driver.family_name).all()
                
                if not drivers:
                    logger.warning("⚠️ No se encontraron pilotos en la base de datos")
                    return pd.DataFrame()
                
                df = pd.DataFrame([
                    {
                        'DriverID': driver.driver_id,
                        'Code': driver.code,
                        'GivenName': driver.given_name,
                        'FamilyName': driver.family_name,
                        'FullName': driver.full_name,
                        'DateOfBirth': driver.date_of_birth,
                        'Nationality': driver.nationality,
                        'PermanentNumber': driver.permanent_number
                    }
                    for driver in drivers
                ])
                
                logger.info(f"✅ Pilotos extraídos: {len(df)} registros")
                return df
                
        except Exception as e:
            logger.error(f"❌ Error extrayendo pilotos: {e}")
            return None
    
    def extract_constructors(self):
        """
        Extrae la lista de todos los constructores desde la base de datos.
        
        Returns:
            pandas.DataFrame: DataFrame con información de constructores
        """
        try:
            with get_session() as session:
                constructors = session.query(Constructor).order_by(Constructor.name).all()
                
                if not constructors:
                    logger.warning("⚠️ No se encontraron constructores en la base de datos")
                    return pd.DataFrame()
                
                df = pd.DataFrame([
                    {
                        'ConstructorID': constructor.constructor_id,
                        'Name': constructor.name,
                        'Nationality': constructor.nationality
                    }
                    for constructor in constructors
                ])
                
                logger.info(f"✅ Constructores extraídos: {len(df)} registros")
                return df
                
        except Exception as e:
            logger.error(f"❌ Error extrayendo constructores: {e}")
            return None