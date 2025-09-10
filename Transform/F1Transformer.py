#!/usr/bin/env python3
"""
Transformador de datos F1 - Versión 2.0 con SQLAlchemy
=======================================================

Transforma y limpia datos tanto en pandas como con objetos SQLAlchemy.

Autor: Miguel Bonilla
Fecha: Septiembre 2025
"""

import pandas as pd
from datetime import datetime
from Database import get_session
from Models import Driver, Constructor, QualifyingResult
import logging

logger = logging.getLogger(__name__)

class Transformer:
    """
    Clase para transformar y limpiar los datos extraídos.
    Ahora incluye capacidades SQLAlchemy para trabajar con objetos ORM.
    """
    def __init__(self, df):
        self.df = df

    def clean(self):
        """
        Realiza limpieza y transformación de los datos (método original).
        """
        df = self.df.copy()
        # Limpieza de columnas de fecha y hora
        df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
        df['Time'] = pd.to_datetime(df['Time'], errors='coerce').dt.time
        # Eliminar filas con Booking ID nulo
        df = df.dropna(subset=['Booking ID'])
        # Rellenar valores nulos en columnas numéricas con 0
        num_cols = ['Avg VTAT', 'Avg CTAT', 'Booking Value', 'Ride Distance', 'Driver Ratings', 'Customer Rating']
        for col in num_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
        # Rellenar valores nulos en columnas de texto con 'Unknown'
        text_cols = ['Booking Status', 'Vehicle Type', 'Pickup Location', 'Drop Location',
                     'Reason for cancelling by Customer', 'Driver Cancellation Reason',
                     'Incomplete Rides Reason', 'Payment Method']
        for col in text_cols:
            if col in df.columns:
                df[col] = df[col].fillna('Unknown')
        # Convertir flags a booleanos
        flag_cols = ['Cancelled Rides by Customer', 'Cancelled Rides by Driver', 'Incomplete Rides']
        for col in flag_cols:
            if col in df.columns:
                df[col] = df[col].astype(bool)
        self.df = df
        return self.df

    def clean_qualifying_data(self):
        """
        Realiza limpieza específica para los datos de qualifying.
        Genera el campo 'Code' con las primeras 3 letras del 'FamilyName'.
        """
        df = self.df.copy()
        
        # Verificar si existe la columna FamilyName
        if 'FamilyName' in df.columns:
            # Generar el código con las primeras 3 letras del apellido en mayúsculas
            df['Code'] = df['FamilyName'].apply(lambda x: str(x)[:3].upper() if pd.notna(x) and str(x).strip() != '' else '')
            
            # Limpiar espacios en blanco en columnas de texto
            text_columns = ['FamilyName', 'GivenName', 'Nationality', 'ConstructorName', 'ConstructorNationality']
            for col in text_columns:
                if col in df.columns:
                    df[col] = df[col].astype(str).str.strip()
            
            # Convertir columnas de tiempo a formato numérico si es necesario
            time_columns = ['Q1', 'Q2', 'Q3']
            for col in time_columns:
                if col in df.columns:
                    # Mantener como string si ya está en formato de tiempo (mm:ss.sss)
                    # Si son ceros, mantenerlos como están
                    df[col] = df[col].astype(str)
            
            # Convertir columnas numéricas
            numeric_columns = ['Season', 'Round', 'Position', 'PermanentNumber']
            for col in numeric_columns:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
            
            # Limpiar y convertir fecha de nacimiento
            if 'DateOfBirth' in df.columns:
                df['DateOfBirth'] = pd.to_datetime(df['DateOfBirth'], errors='coerce')
        
        self.df = df
        logger.info(f"✅ Datos de qualifying limpios: {len(df)} registros")
        return self.df
    
    def transform_to_orm_objects(self):
        """
        Transforma el DataFrame pandas a objetos SQLAlchemy ORM.
        
        Returns:
            tuple: (lista_drivers, lista_constructors, lista_qualifying_results)
        """
        try:
            if self.df is None or self.df.empty:
                logger.warning("⚠️ No hay datos para transformar")
                return [], [], []
            
            # Asegurar que los datos estén limpios
            df_clean = self.clean_qualifying_data()
            
            drivers = {}
            constructors = {}
            qualifying_results = []
            
            logger.info(f"🔄 Transformando {len(df_clean)} registros a objetos ORM...")
            
            for index, row in df_clean.iterrows():
                try:
                    # Procesar Driver
                    driver_id = str(row.get('DriverID', '')).strip()
                    if driver_id and driver_id not in drivers:
                        # Generar código si no existe
                        code = row.get('Code', '')
                        if not code and row.get('FamilyName'):
                            code = str(row['FamilyName'])[:3].upper()
                        
                        # Procesar fecha de nacimiento
                        date_of_birth = None
                        if pd.notna(row.get('DateOfBirth')):
                            try:
                                if isinstance(row['DateOfBirth'], str):
                                    date_of_birth = datetime.strptime(row['DateOfBirth'], '%Y-%m-%d').date()
                                else:
                                    date_of_birth = row['DateOfBirth'].date()
                            except (ValueError, AttributeError):
                                date_of_birth = None
                        
                        driver = Driver(
                            driver_id=driver_id,
                            permanent_number=int(row['PermanentNumber']) if pd.notna(row.get('PermanentNumber')) and row['PermanentNumber'] != 0 else None,
                            code=code if code else None,
                            given_name=str(row.get('GivenName', '')).strip(),
                            family_name=str(row.get('FamilyName', '')).strip(),
                            date_of_birth=date_of_birth,
                            nationality=str(row.get('Nationality', '')).strip() if pd.notna(row.get('Nationality')) else None
                        )
                        drivers[driver_id] = driver
                    
                    # Procesar Constructor
                    constructor_id = str(row.get('ConstructorID', '')).strip()
                    if constructor_id and constructor_id not in constructors:
                        constructor = Constructor(
                            constructor_id=constructor_id,
                            name=str(row.get('ConstructorName', '')).strip(),
                            nationality=str(row.get('ConstructorNationality', '')).strip() if pd.notna(row.get('ConstructorNationality')) else None
                        )
                        constructors[constructor_id] = constructor
                    
                    # Procesar QualifyingResult
                    if driver_id and constructor_id:
                        qualifying_result = QualifyingResult(
                            season=int(row['Season']) if pd.notna(row.get('Season')) else None,
                            round=int(row['Round']) if pd.notna(row.get('Round')) else None,
                            circuitid=str(row.get('CircuitID', '')).strip(),
                            position=int(row['Position']) if pd.notna(row.get('Position')) else None,
                            q1=str(row.get('Q1', '')) if pd.notna(row.get('Q1')) else None,
                            q2=str(row.get('Q2', '')) if pd.notna(row.get('Q2')) else None,
                            q3=str(row.get('Q3', '')) if pd.notna(row.get('Q3')) else None,
                            # Las relaciones se establecerán cuando se guarden en BD
                        )
                        # Guardar referencias para establecer relaciones después
                        qualifying_result._driver_id = driver_id
                        qualifying_result._constructor_id = constructor_id
                        qualifying_results.append(qualifying_result)
                
                except Exception as e:
                    logger.warning(f"⚠️ Error procesando fila {index}: {e}")
                    continue
            
            drivers_list = list(drivers.values())
            constructors_list = list(constructors.values())
            
            logger.info("✅ Transformación ORM completada:")
            logger.info(f"   • Pilotos únicos: {len(drivers_list)}")
            logger.info(f"   • Constructores únicos: {len(constructors_list)}")
            logger.info(f"   • Resultados qualifying: {len(qualifying_results)}")
            
            return drivers_list, constructors_list, qualifying_results
            
        except Exception as e:
            logger.error(f"❌ Error en transformación ORM: {e}")
            return [], [], []
    
    def validate_data_quality(self):
        """
        Valida la calidad de los datos transformados.
        
        Returns:
            dict: Reporte de calidad de datos
        """
        if self.df is None or self.df.empty:
            return {"error": "No hay datos para validar"}
        
        df = self.df
        report = {}
        
        # Estadísticas básicas
        report['total_records'] = len(df)
        report['total_columns'] = len(df.columns)
        
        # Valores nulos por columna
        report['null_values'] = df.isnull().sum().to_dict()
        
        # Duplicados
        if 'DriverID' in df.columns and 'Season' in df.columns and 'Round' in df.columns:
            duplicates = df.duplicated(subset=['Season', 'Round', 'DriverID']).sum()
            report['duplicates'] = int(duplicates)
        
        # Códigos de piloto únicos
        if 'Code' in df.columns:
            report['unique_codes'] = df['Code'].nunique()
            report['empty_codes'] = (df['Code'] == '').sum()
        
        # Rango de temporadas
        if 'Season' in df.columns:
            report['season_range'] = {
                'min': int(df['Season'].min()) if pd.notna(df['Season'].min()) else None,
                'max': int(df['Season'].max()) if pd.notna(df['Season'].max()) else None
            }
        
        # Tiempos válidos de qualifying
        time_cols = ['Q1', 'Q2', 'Q3']
        valid_times = 0
        for col in time_cols:
            if col in df.columns:
                valid_times += (df[col].notna() & (df[col] != '0') & df[col].str.contains(':', na=False)).sum()
        report['valid_qualifying_times'] = int(valid_times)
        
        logger.info(f"📊 Reporte de calidad generado: {report['total_records']} registros analizados")
        return report