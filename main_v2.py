#!/usr/bin/env python3
"""
ETL Project 2.0 - Procesamiento de datos de qualifying de Fórmula 1 con SQLAlchemy
==================================================================================

Este script principal ejecuta el pipeline completo ETL con capacidades avanzadas:
1. Extract: Extrae datos de CSV y bases de datos
2. Transform: Limpia y transforma datos con validación de calidad
3. Load: Carga a múltiples destinos (CSV, SQLite, PostgreSQL/Supabase)

Autor: Miguel Bonilla
Fecha: Septiembre 2025
"""

import sys
import os
from datetime import datetime
import logging
import argparse

# Constantes para mensajes comunes
ERROR_NO_DATA_EXTRACTED = "No se pudieron extraer los datos"
ERROR_DETAILED_LOG = "Error detallado:"

# Agregar el directorio actual al path para imports
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Imports de módulos del proyecto
from Extract.F1Extract import Extractor
from Transform.F1Transformer import Transformer
from Load.F1Loader import Loader
from Config.config import Config
from Database import test_db_connection, create_all_tables

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('etl_pipeline.log')
    ]
)
logger = logging.getLogger(__name__)

def print_banner():
    """Imprime el banner del proyecto."""
    print("=" * 70)
    print("🏎️  PROYECTO ETL 2.0 - DATOS QUALIFYING F1 con SQLAlchemy")
    print("=" * 70)
    print(f"Iniciado: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Entorno: {Config.ENVIRONMENT}")
    print(f"Base de datos: {Config.get_database_url()}")
    print("-" * 70)

def print_step(step_number, description):
    """Imprime información del paso actual."""
    print(f"\n📋 PASO {step_number}: {description}")
    print("-" * 50)

def print_success(message):
    """Imprime mensaje de éxito."""
    print(f"✅ {message}")

def print_error(message):
    """Imprime mensaje de error."""
    print(f"❌ ERROR: {message}")

def print_info(message):
    """Imprime información general."""
    print(f"ℹ️  {message}")

def print_warning(message):
    """Imprime mensaje de advertencia."""
    print(f"⚠️  {message}")

def test_database_connection():
    """Prueba la conexión a la base de datos."""
    print_step("0", "VERIFICACIÓN DE CONEXIÓN A BASE DE DATOS")
    
    if test_db_connection():
        print_success("Conexión a base de datos exitosa")
        return True
    else:
        print_error("No se pudo conectar a la base de datos")
        print_info("Verifica la configuración en el archivo .env")
        return False

def main_traditional_pipeline():
    """
    Ejecuta el pipeline ETL tradicional (compatibilidad hacia atrás).
    """
    try:
        print_banner()
        
        # Verificar conexión a BD
        if not test_database_connection():
            return False
        
        # PASO 1: EXTRACCIÓN
        print_step(1, "EXTRACCIÓN DE DATOS")
        print_info(f"Archivo fuente: {Config.INPUT_PATH}")
        
        extractor = Extractor()
        df_raw = extractor.extract_csv(Config.INPUT_PATH)
        
        if df_raw is None:
            print_error(ERROR_NO_DATA_EXTRACTED)
            return False
            
        print_success(f"Datos extraídos: {len(df_raw)} registros")
        print_info(f"Columnas: {list(df_raw.columns)}")
        
        # PASO 2: TRANSFORMACIÓN
        print_step(2, "TRANSFORMACIÓN Y LIMPIEZA")
        
        transformer = Transformer(df_raw)
        df_clean = transformer.clean_qualifying_data()
        
        if df_clean is None:
            print_error("Error en la transformación de datos")
            return False
            
        print_success("Datos transformados exitosamente")
        print_info(f"Registros procesados: {len(df_clean)}")
        print_info(f"Códigos únicos generados: {df_clean['Code'].nunique()}")
        
        # Mostrar muestra de datos transformados
        print("\n📊 MUESTRA DE DATOS TRANSFORMADOS:")
        print(df_clean[['Season', 'Round', 'GivenName', 'FamilyName', 'Code', 'ConstructorName']].head(10).to_string())
        
        # PASO 3: CARGA
        print_step(3, "CARGA DE DATOS")
        
        loader = Loader(df=df_clean)
        
        # Crear directorio de salida si no existe
        output_dir = "Output"
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
            print_info(f"Directorio creado: {output_dir}")
        
        # Generar nombres de archivo con timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        csv_output = f"{output_dir}/qualifying_results_clean_{timestamp}.csv"
        
        # Cargar a CSV
        print_info("Guardando en formato CSV...")
        if loader.to_csv(csv_output):
            print_success(f"Archivo CSV guardado: {csv_output}")
        
        # Cargar a SQLite (método tradicional)
        print_info("Guardando en base de datos SQLite (método tradicional)...")
        if loader.to_sqlite():
            print_success(f"Datos guardados en SQLite: {Config.SQLITE_DB_PATH}")
            print_info(f"Tabla: {Config.SQLITE_TABLE}")
        
        return True
        
    except Exception as e:
        print_error(f"Error inesperado en el pipeline tradicional: {e}")
        logger.exception(ERROR_DETAILED_LOG)
        return False

def main_sqlalchemy_pipeline():
    """
    Ejecuta el pipeline ETL usando SQLAlchemy ORM.
    """
    try:
        print_banner()
        
        # Verificar conexión a BD
        if not test_database_connection():
            return False
        
        # PASO 1: EXTRACCIÓN
        print_step(1, "EXTRACCIÓN DE DATOS")
        print_info(f"Archivo fuente: {Config.INPUT_PATH}")
        
        extractor = Extractor()
        df_raw = extractor.extract_csv(Config.INPUT_PATH)
        
        if df_raw is None:
            print_error(ERROR_NO_DATA_EXTRACTED)
            return False
            
        print_success(f"Datos extraídos: {len(df_raw)} registros")
        
        # PASO 2: TRANSFORMACIÓN
        print_step(2, "TRANSFORMACIÓN Y VALIDACIÓN DE CALIDAD")
        
        transformer = Transformer(df_raw)
        df_clean = transformer.clean_qualifying_data()
        
        if df_clean is None:
            print_error("Error en la transformación de datos")
            return False
        
        # Generar reporte de calidad
        quality_report = transformer.validate_data_quality()
        print_success("Datos transformados exitosamente")
        print_info("📊 Reporte de calidad de datos:")
        for key, value in quality_report.items():
            if key != 'null_values':
                print_info(f"   • {key}: {value}")
        
        # Transformar a objetos ORM
        print_info("🔄 Transformando a objetos SQLAlchemy...")
        drivers, constructors, qualifying_results = transformer.transform_to_orm_objects()
        
        # PASO 3: CARGA CON SQLALCHEMY
        print_step(3, "CARGA DE DATOS CON SQLALCHEMY ORM")
        
        loader = Loader(
            df=df_clean,
            drivers=drivers,
            constructors=constructors,
            qualifying_results=qualifying_results
        )
        
        # Crear directorio de salida si no existe
        output_dir = "Output"
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        csv_output = f"{output_dir}/qualifying_results_sqlalchemy_{timestamp}.csv"
        
        # Cargar a CSV
        print_info("💾 Guardando en formato CSV...")
        if loader.to_csv(csv_output):
            print_success(f"Archivo CSV guardado: {csv_output}")
        
        # Cargar usando SQLAlchemy ORM
        print_info("🗃️  Guardando en base de datos usando SQLAlchemy ORM...")
        stats = loader.to_database_orm()
        
        if 'error' not in stats:
            print_success("Datos guardados exitosamente usando SQLAlchemy ORM")
        else:
            print_error(f"Error en carga ORM: {stats['error']}")
            return False
        
        # RESUMEN FINAL
        print("\n" + "=" * 70)
        print("🎉 PIPELINE ETL 2.0 COMPLETADO EXITOSAMENTE")
        print("=" * 70)
        print("📈 Estadísticas finales:")
        print(f"   • Registros procesados: {len(df_clean):,}")
        print(f"   • Pilotos únicos: {len(drivers)}")
        print(f"   • Constructores únicos: {len(constructors)}")
        print(f"   • Resultados qualifying: {len(qualifying_results)}")
        print(f"   • Temporadas: {df_clean['Season'].min()} - {df_clean['Season'].max()}")
        
        print("\n📁 Archivos generados:")
        print(f"   • CSV: {csv_output}")
        print(f"   • Base de datos: {Config.get_database_url()}")
        
        print(f"\n⏱️  Completado: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        return True
        
    except Exception as e:
        print_error(f"Error inesperado en el pipeline SQLAlchemy: {e}")
        logger.exception(ERROR_DETAILED_LOG)
        return False

def load_to_supabase():
    """
    Carga datos específicamente a Supabase.
    """
    try:
        print_banner()
        print_step("SUPABASE", "CARGA DE DATOS A SUPABASE")
        
        if not Config.get_database_url().startswith('postgresql'):
            print_warning("La URL de base de datos no apunta a PostgreSQL")
            print_info("Para usar Supabase, configura DATABASE_URL en .env:")
            print_info("DATABASE_URL=postgresql://postgres:[PASSWORD]@db.[PROJECT-REF].supabase.co:5432/postgres")
            return False
        
        # Extraer datos desde archivo local
        extractor = Extractor()
        df_raw = extractor.extract_csv(Config.INPUT_PATH)
        
        if df_raw is None:
            print_error(ERROR_NO_DATA_EXTRACTED)
            return False
        
        # Transformar datos
        transformer = Transformer(df_raw)
        df_clean = transformer.clean_qualifying_data()
        drivers, constructors, qualifying_results = transformer.transform_to_orm_objects()
        
        # Cargar a Supabase
        loader = Loader(
            df=df_clean,
            drivers=drivers,
            constructors=constructors,
            qualifying_results=qualifying_results
        )
        
        if loader.to_supabase():
            print_success("Datos cargados exitosamente a Supabase")
            return True
        else:
            print_error("Error cargando datos a Supabase")
            return False
            
    except Exception as e:
        print_error(f"Error en carga a Supabase: {e}")
        logger.exception(ERROR_DETAILED_LOG)
        return False

def show_data_sample():
    """
    Muestra una muestra de los datos procesados desde la base de datos.
    """
    try:
        print_banner()
        print("📊 VISUALIZACIÓN DE DATOS PROCESADOS")
        
        extractor = Extractor()
        df = extractor.extract_from_database(limit=20)
        
        if df is not None and not df.empty:
            print(f"\nÚltimos datos en la base de datos ({len(df)} registros):")
            print("-" * 70)
            print(df[['Season', 'Round', 'GivenName', 'FamilyName', 'Code', 'ConstructorName', 'Position']].to_string())
            
            print("\nEstadísticas rápidas:")
            print(f"• Temporadas: {sorted(df['Season'].unique())}")
            print(f"• Constructores únicos: {df['ConstructorName'].nunique()}")
            print(f"• Pilotos únicos: {df['Code'].nunique()}")
        else:
            print_info("No hay datos en la base de datos. Ejecuta el ETL primero.")
            
    except Exception as e:
        print_error(f"Error al mostrar datos: {e}")
        logger.exception(ERROR_DETAILED_LOG)

def create_tables():
    """
    Crea las tablas en la base de datos.
    """
    try:
        print_banner()
        print_step("SETUP", "CREACIÓN DE TABLAS EN BASE DE DATOS")
        
        if test_db_connection():
            create_all_tables()
            print_success("Tablas creadas exitosamente")
            return True
        else:
            print_error("No se pudo conectar a la base de datos")
            return False
            
    except Exception as e:
        print_error(f"Error creando tablas: {e}")
        logger.exception(ERROR_DETAILED_LOG)
        return False

def main():
    """
    Función principal con argumentos de línea de comandos.
    """
    parser = argparse.ArgumentParser(description='ETL Pipeline F1 con SQLAlchemy')
    parser.add_argument('command', nargs='?', default='sqlalchemy',
                       choices=['traditional', 'sqlalchemy', 'supabase', 'show', 'create-tables', 'help'],
                       help='Comando a ejecutar')
    parser.add_argument('--debug', action='store_true', help='Habilitar modo debug')
    
    args = parser.parse_args()
    
    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)
    
    command = args.command.lower()
    
    if command == 'traditional':
        success = main_traditional_pipeline()
    elif command == 'sqlalchemy':
        success = main_sqlalchemy_pipeline()
    elif command == 'supabase':
        success = load_to_supabase()
    elif command == 'show':
        show_data_sample()
        success = True
    elif command == 'create-tables':
        success = create_tables()
    elif command == 'help':
        print("🏎️  ETL Project 2.0 - Procesamiento de datos F1 con SQLAlchemy")
        print("\nComandos disponibles:")
        print("  traditional    - Ejecuta el pipeline ETL tradicional (pandas + sqlite3)")
        print("  sqlalchemy     - Ejecuta el pipeline ETL con SQLAlchemy ORM (por defecto)")
        print("  supabase       - Carga datos específicamente a Supabase")
        print("  show           - Muestra muestra de datos procesados")
        print("  create-tables  - Crea las tablas en la base de datos")
        print("  help           - Muestra esta ayuda")
        print("\nOpciones:")
        print("  --debug        - Habilita logging detallado")
        print("\nEjemplos:")
        print("  python main_v2.py sqlalchemy")
        print("  python main_v2.py supabase --debug")
        print("  python main_v2.py show")
        success = True
    else:
        print_error(f"Comando desconocido: {command}")
        print("Usa 'python main_v2.py help' para ver comandos disponibles")
        success = False
    
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()
