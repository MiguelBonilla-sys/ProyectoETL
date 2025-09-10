#!/usr/bin/env python3
"""
Configuración de la base de datos SQLAlchemy
============================================

Este módulo configura la conexión a la base de datos usando SQLAlchemy.
Soporta tanto SQLite (desarrollo) como PostgreSQL (Supabase producción).

Autor: Miguel Bonilla
Fecha: Septiembre 2025
"""

from sqlalchemy import create_engine, event, text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.pool import StaticPool
from contextlib import contextmanager
import logging
from typing import Generator

from Config.config import Config

# Logger para database
logger = logging.getLogger(__name__)

# Base declarativa para todos los modelos - importar desde models.base para evitar circular imports
# Base = declarative_base()  # Comentado - ahora se define en models.base

class DatabaseManager:
    """
    Gestor de la base de datos con SQLAlchemy.
    Maneja conexiones, sesiones y configuración de engines.
    """
    
    def __init__(self):
        self._engine = None
        self._session_factory = None
        self._setup_engine()
    
    def _setup_engine(self):
        """Configura el engine de SQLAlchemy según el entorno."""
        database_url = Config.get_database_url()
        
        # Configuración específica según el tipo de base de datos
        if database_url.startswith('sqlite'):
            # Configuración para SQLite
            engine_kwargs = {
                'echo': Config.SQLALCHEMY_ECHO,
                'poolclass': StaticPool,
                'pool_pre_ping': True,
                'connect_args': {
                    'check_same_thread': False,  # Permite uso desde múltiples threads
                    'timeout': 20
                }
            }
        elif database_url.startswith('postgresql'):
            # Configuración para PostgreSQL (Supabase)
            engine_kwargs = {
                'echo': Config.SQLALCHEMY_ECHO,
                'pool_size': Config.SQLALCHEMY_POOL_SIZE,
                'max_overflow': Config.SQLALCHEMY_MAX_OVERFLOW,
                'pool_pre_ping': True,
                'pool_recycle': 3600,  # Reciclar conexiones cada hora
                'connect_args': {
                    'connect_timeout': 10,
                    'application_name': 'F1_ETL_Project'
                }
            }
        else:
            # Configuración por defecto
            engine_kwargs = {
                'echo': Config.SQLALCHEMY_ECHO,
                'pool_pre_ping': True
            }
        
        try:
            self._engine = create_engine(database_url, **engine_kwargs)
            
            # Configurar eventos para SQLite
            if database_url.startswith('sqlite'):
                self._setup_sqlite_events()
            
            # Crear session factory
            self._session_factory = sessionmaker(
                bind=self._engine,
                autocommit=False,
                autoflush=False,
                expire_on_commit=False
            )
            
            logger.info(f"✅ Engine configurado exitosamente para: {database_url}")
            
        except Exception as e:
            logger.error(f"❌ Error configurando engine de base de datos: {e}")
            raise
    
    def _setup_sqlite_events(self):
        """Configura eventos específicos para SQLite."""
        @event.listens_for(self._engine, "connect")
        def set_sqlite_pragma(dbapi_connection, connection_record):
            """Configura pragmas de SQLite para mejor rendimiento."""
            cursor = dbapi_connection.cursor()
            # Habilitar foreign keys
            cursor.execute("PRAGMA foreign_keys=ON")
            # Optimizar para escritura
            cursor.execute("PRAGMA journal_mode=WAL")
            cursor.execute("PRAGMA synchronous=NORMAL")
            cursor.execute("PRAGMA cache_size=10000")
            cursor.execute("PRAGMA temp_store=MEMORY")
            cursor.close()
    
    @property
    def engine(self):
        """Retorna el engine de SQLAlchemy."""
        if self._engine is None:
            raise RuntimeError("Engine no configurado. Llamar _setup_engine() primero.")
        return self._engine
    
    @contextmanager
    def get_session(self) -> Generator[Session, None, None]:
        """
        Context manager para obtener una sesión de base de datos.
        
        Uso:
            with db_manager.get_session() as session:
                # usar session
                session.add(objeto)
                session.commit()
        """
        if self._session_factory is None:
            raise RuntimeError("Session factory no configurado.")
        
        session = self._session_factory()
        try:
            yield session
            session.commit()
        except Exception as e:
            session.rollback()
            logger.error(f"Error en sesión de base de datos: {e}")
            raise
        finally:
            session.close()
    
    def create_tables(self):
        """Crea todas las tablas definidas en los modelos."""
        try:
            # Import aquí para evitar circular imports
            from Models.base import Base
            Base.metadata.create_all(bind=self._engine)
            logger.info("✅ Tablas creadas exitosamente")
        except Exception as e:
            logger.error(f"❌ Error creando tablas: {e}")
            raise
    
    def drop_tables(self):
        """Elimina todas las tablas (usar con cuidado)."""
        try:
            # Import aquí para evitar circular imports
            from Models.base import Base
            Base.metadata.drop_all(bind=self._engine)
            logger.info("⚠️  Tablas eliminadas")
        except Exception as e:
            logger.error(f"❌ Error eliminando tablas: {e}")
            raise
    
    def test_connection(self) -> bool:
        """Prueba la conexión a la base de datos."""
        try:
            with self.get_session() as session:
                session.execute(text("SELECT 1"))
            logger.info("✅ Conexión a base de datos exitosa")
            return True
        except Exception as e:
            logger.error(f"❌ Error probando conexión: {e}")
            return False

# Instancia global del gestor de base de datos
db_manager = DatabaseManager()

# Funciones de conveniencia
def get_session() -> Generator[Session, None, None]:
    """Función de conveniencia para obtener una sesión."""
    return db_manager.get_session()

def get_engine():
    """Función de conveniencia para obtener el engine."""
    return db_manager.engine

def create_all_tables():
    """Función de conveniencia para crear todas las tablas."""
    return db_manager.create_tables()

def test_db_connection() -> bool:
    """Función de conveniencia para probar la conexión."""
    return db_manager.test_connection()
