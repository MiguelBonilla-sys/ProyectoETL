
#!/usr/bin/env python3
"""
Loader de datos F1 - Versión 2.0 con SQLAlchemy
===============================================

Carga datos tanto en formato tradicional (CSV, SQLite) como usando SQLAlchemy ORM.

Autor: Miguel Bonilla
Fecha: Septiembre 2025
"""

from Config.config import Config
import sqlite3
import os
import pandas as pd
from Database import get_session, create_all_tables
from Models import Driver, Constructor, QualifyingResult
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

class Loader:
    """
    Clase para cargar los datos limpios a un destino.
    Ahora incluye capacidades SQLAlchemy para guardar objetos ORM.
    """
    def __init__(self, df=None, drivers=None, constructors=None, qualifying_results=None):
        self.df = df
        self.drivers = drivers or []
        self.constructors = constructors or []
        self.qualifying_results = qualifying_results or []

    def to_csv(self, output_path):
        """
        Guarda el DataFrame limpio en un archivo CSV.
        """
        try:
            if self.df is None:
                logger.error("❌ No hay DataFrame para guardar en CSV")
                return False
                
            self.df.to_csv(output_path, index=False)
            logger.info(f"✅ Datos guardados en CSV: {output_path}")
            return True
        except Exception as e:
            logger.error(f"❌ Error al guardar datos en CSV: {e}")
            return False

    def to_sqlite(self, db_path=None, table_name=None):
        """
        Guarda el DataFrame limpio en una base de datos SQLite (método original).
        """
        db_path = db_path or Config.SQLITE_DB_PATH
        table_name = table_name or Config.SQLITE_TABLE
        
        try:
            if self.df is None:
                logger.error("❌ No hay DataFrame para guardar en SQLite")
                return False
                
            # Crear directorio si no existe
            db_dir = os.path.dirname(db_path)
            if db_dir and not os.path.exists(db_dir):
                os.makedirs(db_dir)
                logger.info(f"📁 Directorio creado para BD: {db_dir}")
            
            # Verificar que el directorio existe y es escribible
            if not os.path.exists(db_dir):
                raise OSError(f"No se puede crear el directorio: {db_dir}")
            
            # Crear conexión a la base de datos
            conn = sqlite3.connect(db_path)
            self.df.to_sql(table_name, conn, if_exists='replace', index=False)
            conn.close()
            logger.info(f"✅ Datos guardados en SQLite: {db_path}, tabla: {table_name}")
            
            # Verificar que el archivo se creó correctamente
            if os.path.exists(db_path):
                size = os.path.getsize(db_path)
                logger.info(f"📊 Archivo DB creado exitosamente. Tamaño: {size:,} bytes")
            else:
                logger.warning("⚠️ El archivo de base de datos no se encontró después de la creación")
                
            return True
                
        except Exception as e:
            logger.error(f"❌ Error al guardar en SQLite: {e}")
            logger.error(f"Ruta intentada: {db_path}")
            logger.error(f"Directorio padre: {os.path.dirname(db_path)}")
            
            # Intentar guardar en directorio actual como fallback
            fallback_path = f"f1_data_{table_name}.db"
            try:
                logger.info(f"🔄 Intentando guardar en directorio actual: {fallback_path}")
                conn = sqlite3.connect(fallback_path)
                self.df.to_sql(table_name, conn, if_exists='replace', index=False)
                conn.close()
                logger.info(f"✅ Datos guardados exitosamente en: {fallback_path}")
                return True
            except Exception as fallback_error:
                logger.error(f"❌ Error en fallback: {fallback_error}")
                return False
    
    def to_database_orm(self, create_tables=True):
        """
        Guarda los datos usando SQLAlchemy ORM.
        
        Args:
            create_tables (bool): Si crear las tablas antes de insertar datos
            
        Returns:
            dict: Estadísticas de la operación
        """
        try:
            # Crear tablas si se solicita
            if create_tables:
                logger.info("🔧 Creando tablas en la base de datos...")
                create_all_tables()
            
            stats = {
                'drivers_created': 0,
                'drivers_updated': 0,
                'constructors_created': 0,
                'constructors_updated': 0,
                'qualifying_results_created': 0,
                'errors': 0
            }
            
            with get_session() as session:
                # Procesar Drivers
                logger.info(f"🔄 Procesando {len(self.drivers)} pilotos...")
                driver_map = {}  # Para mapear driver_id a objeto Driver en BD
                
                for driver in self.drivers:
                    try:
                        existing_driver = session.query(Driver).filter_by(
                            driver_id=driver.driver_id
                        ).first()
                        
                        if existing_driver:
                            # Actualizar driver existente
                            existing_driver.permanent_number = driver.permanent_number
                            existing_driver.code = driver.code
                            existing_driver.given_name = driver.given_name
                            existing_driver.family_name = driver.family_name
                            existing_driver.date_of_birth = driver.date_of_birth
                            existing_driver.nationality = driver.nationality
                            driver_map[driver.driver_id] = existing_driver
                            stats['drivers_updated'] += 1
                        else:
                            # Crear nuevo driver
                            session.add(driver)
                            session.flush()  # Para obtener el ID
                            driver_map[driver.driver_id] = driver
                            stats['drivers_created'] += 1
                            
                    except Exception as e:
                        logger.warning(f"⚠️ Error procesando driver {driver.driver_id}: {e}")
                        stats['errors'] += 1
                
                # Procesar Constructors
                logger.info(f"🔄 Procesando {len(self.constructors)} constructores...")
                constructor_map = {}  # Para mapear constructor_id a objeto Constructor en BD
                
                for constructor in self.constructors:
                    try:
                        existing_constructor = session.query(Constructor).filter_by(
                            constructor_id=constructor.constructor_id
                        ).first()
                        
                        if existing_constructor:
                            # Actualizar constructor existente
                            existing_constructor.name = constructor.name
                            existing_constructor.nationality = constructor.nationality
                            constructor_map[constructor.constructor_id] = existing_constructor
                            stats['constructors_updated'] += 1
                        else:
                            # Crear nuevo constructor
                            session.add(constructor)
                            session.flush()  # Para obtener el ID
                            constructor_map[constructor.constructor_id] = constructor
                            stats['constructors_created'] += 1
                            
                    except Exception as e:
                        logger.warning(f"⚠️ Error procesando constructor {constructor.constructor_id}: {e}")
                        stats['errors'] += 1
                
                # Procesar QualifyingResults
                logger.info(f"🔄 Procesando {len(self.qualifying_results)} resultados de qualifying...")
                
                for qualifying_result in self.qualifying_results:
                    try:
                        # Establecer relaciones con Driver y Constructor
                        driver_id = getattr(qualifying_result, '_driver_id', None)
                        constructor_id = getattr(qualifying_result, '_constructor_id', None)
                        
                        if driver_id in driver_map and constructor_id in constructor_map:
                            qualifying_result.driver_db_id = driver_map[driver_id].id
                            qualifying_result.constructor_db_id = constructor_map[constructor_id].id
                            
                            # Verificar si ya existe este resultado
                            existing_result = session.query(QualifyingResult).filter_by(
                                season=qualifying_result.season,
                                round=qualifying_result.round,
                                driver_db_id=qualifying_result.driver_db_id
                            ).first()
                            
                            if existing_result:
                                # Actualizar resultado existente
                                existing_result.circuitid = qualifying_result.circuitid
                                existing_result.position = qualifying_result.position
                                existing_result.q1 = qualifying_result.q1
                                existing_result.q2 = qualifying_result.q2
                                existing_result.q3 = qualifying_result.q3
                                existing_result.constructor_db_id = qualifying_result.constructor_db_id
                            else:
                                # Crear nuevo resultado
                                session.add(qualifying_result)
                                stats['qualifying_results_created'] += 1
                        else:
                            logger.warning(f"⚠️ No se pudo establecer relación para resultado: driver_id={driver_id}, constructor_id={constructor_id}")
                            stats['errors'] += 1
                            
                    except Exception as e:
                        logger.warning(f"⚠️ Error procesando resultado de qualifying: {e}")
                        stats['errors'] += 1
                
                # Commit final
                session.commit()
                
            logger.info("✅ Datos guardados exitosamente en la base de datos usando ORM")
            logger.info("📊 Estadísticas de carga:")
            logger.info(f"   • Pilotos creados: {stats['drivers_created']}")
            logger.info(f"   • Pilotos actualizados: {stats['drivers_updated']}")
            logger.info(f"   • Constructores creados: {stats['constructors_created']}")
            logger.info(f"   • Constructores actualizados: {stats['constructors_updated']}")
            logger.info(f"   • Resultados qualifying creados: {stats['qualifying_results_created']}")
            logger.info(f"   • Errores: {stats['errors']}")
            
            return stats
            
        except Exception as e:
            logger.error(f"❌ Error general en carga ORM: {e}")
            return {'error': str(e)}
    
    def to_supabase(self):
        """
        Carga datos específicamente a Supabase (PostgreSQL).
        Utiliza la misma lógica ORM pero con configuraciones específicas para Supabase.
        """
        try:
            if not Config.get_database_url().startswith('postgresql'):
                logger.warning("⚠️ La URL de base de datos no apunta a PostgreSQL (Supabase)")
                logger.info("💡 Asegúrate de configurar DATABASE_URL en .env para Supabase")
                return False
            
            logger.info("🚀 Iniciando carga a Supabase...")
            stats = self.to_database_orm(create_tables=True)
            
            if 'error' not in stats:
                logger.info("✅ Datos cargados exitosamente a Supabase")
                return True
            else:
                logger.error(f"❌ Error cargando a Supabase: {stats['error']}")
                return False
                
        except Exception as e:
            logger.error(f"❌ Error en carga a Supabase: {e}")
            return False
    
    def bulk_load_from_dataframe(self, df=None, batch_size=1000):
        """
        Carga masiva eficiente desde DataFrame usando SQLAlchemy.
        
        Args:
            df (DataFrame, optional): DataFrame a cargar. Si no se proporciona, usa self.df
            batch_size (int): Tamaño del lote para procesamiento
            
        Returns:
            dict: Estadísticas de la operación
        """
        try:
            df_to_load = df if df is not None else self.df
            
            if df_to_load is None or df_to_load.empty:
                logger.error("❌ No hay datos para carga masiva")
                return {'error': 'No data to load'}
            
            logger.info(f"🚀 Iniciando carga masiva de {len(df_to_load)} registros en lotes de {batch_size}...")
            
            # Procesar en lotes
            total_batches = (len(df_to_load) + batch_size - 1) // batch_size
            stats = {'total_processed': 0, 'errors': 0, 'batches': 0}
            
            for i in range(0, len(df_to_load), batch_size):
                batch_df = df_to_load.iloc[i:i + batch_size]
                batch_num = (i // batch_size) + 1
                
                logger.info(f"🔄 Procesando lote {batch_num}/{total_batches} ({len(batch_df)} registros)...")
                
                try:
                    # Crear transformador para este lote
                    from Transform.F1Transformer import Transformer
                    transformer = Transformer(batch_df)
                    drivers, constructors, qualifying_results = transformer.transform_to_orm_objects()
                    
                    # Crear loader para este lote
                    batch_loader = Loader(
                        drivers=drivers,
                        constructors=constructors,
                        qualifying_results=qualifying_results
                    )
                    
                    # Cargar lote
                    batch_stats = batch_loader.to_database_orm(create_tables=(batch_num == 1))
                    
                    if 'error' not in batch_stats:
                        stats['total_processed'] += len(batch_df)
                        stats['batches'] += 1
                    else:
                        stats['errors'] += len(batch_df)
                        logger.error(f"❌ Error en lote {batch_num}: {batch_stats['error']}")
                    
                except Exception as e:
                    logger.error(f"❌ Error procesando lote {batch_num}: {e}")
                    stats['errors'] += len(batch_df)
            
            logger.info("✅ Carga masiva completada:")
            logger.info(f"   • Registros procesados: {stats['total_processed']}")
            logger.info(f"   • Lotes exitosos: {stats['batches']}")
            logger.info(f"   • Errores: {stats['errors']}")
            
            return stats
            
        except Exception as e:
            logger.error(f"❌ Error en carga masiva: {e}")
            return {'error': str(e)}