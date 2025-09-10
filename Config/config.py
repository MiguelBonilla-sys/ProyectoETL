import os
from dotenv import load_dotenv

# Cargar variables de entorno
load_dotenv()

class Config:
    """
    Clase de configuración para rutas y parámetros del ETL.
    Ahora incluye configuración para SQLAlchemy y bases de datos externas.
    """
    # Configuración original (compatibilidad hacia atrás)
    INPUT_PATH = r'Extract\Files\qualifying_results.csv'
    SQLITE_DB_PATH = r'Extract\Files\f1_data.db'
    SQLITE_TABLE = 'qualifying_results'
    
    # Nueva configuración SQLAlchemy
    DATABASE_URL = os.getenv('DATABASE_URL', 'sqlite:///f1_orm.db')  # Base de datos separada para ORM
    ENVIRONMENT = os.getenv('ENVIRONMENT', 'development')
    DEBUG = os.getenv('DEBUG', 'True').lower() == 'true'
    
    # Configuración Supabase (opcional)
    SUPABASE_URL = os.getenv('SUPABASE_URL')
    SUPABASE_ANON_KEY = os.getenv('SUPABASE_ANON_KEY')
    SUPABASE_SERVICE_ROLE_KEY = os.getenv('SUPABASE_SERVICE_ROLE_KEY')
    
    # Configuración SQLAlchemy
    SQLALCHEMY_ECHO = DEBUG  # Mostrar SQL queries en modo debug
    SQLALCHEMY_POOL_SIZE = 10
    SQLALCHEMY_MAX_OVERFLOW = 20
    
    @classmethod
    def get_database_url(cls):
        """Retorna la URL de base de datos configurada."""
        return cls.DATABASE_URL
    
    @classmethod
    def is_production(cls):
        """Verifica si estamos en entorno de producción."""
        return cls.ENVIRONMENT.lower() == 'production'
    
    @classmethod
    def is_development(cls):
        """Verifica si estamos en entorno de desarrollo."""
        return cls.ENVIRONMENT.lower() == 'development'