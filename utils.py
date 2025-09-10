#!/usr/bin/env python3
"""
Utilidades para el proyecto ETL F1
==================================

Script de utilidades para tareas comunes del proyecto.

Autor: Miguel Bonilla
Fecha: Septiembre 2025
"""

import sys
import os
import argparse
from datetime import datetime

# Agregar el directorio actual al path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from Config.config import Config
from Database import test_db_connection, create_all_tables, get_session
from Models import Driver, Constructor, QualifyingResult

def print_banner():
    """Imprime el banner de utilidades."""
    print("=" * 60)
    print("🔧 UTILIDADES ETL F1 - SQLAlchemy")
    print("=" * 60)
    print(f"Fecha: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Entorno: {Config.ENVIRONMENT}")
    print(f"Base de datos: {Config.get_database_url()}")
    print("-" * 60)

def check_database():
    """Verifica el estado de la base de datos."""
    print("🔍 VERIFICACIÓN DE BASE DE DATOS")
    print("-" * 40)
    
    # Probar conexión
    if test_db_connection():
        print("✅ Conexión exitosa")
        
        # Contar registros en cada tabla
        try:
            with get_session() as session:
                driver_count = session.query(Driver).count()
                constructor_count = session.query(Constructor).count()
                qualifying_count = session.query(QualifyingResult).count()
                
                print("📊 Estadísticas de datos:")
                print(f"   • Pilotos: {driver_count:,}")
                print(f"   • Constructores: {constructor_count:,}")
                print(f"   • Resultados qualifying: {qualifying_count:,}")
                
                if qualifying_count > 0:
                    # Mostrar rango de temporadas
                    result = session.query(
                        QualifyingResult.season.distinct()
                    ).order_by(QualifyingResult.season).all()
                    seasons = [r[0] for r in result]
                    print(f"   • Temporadas: {min(seasons)} - {max(seasons)}")
                
        except Exception as e:
            print(f"⚠️  Error consultando datos: {e}")
    else:
        print("❌ Error de conexión")

def setup_database():
    """Configura la base de datos creando todas las tablas."""
    print("🔧 CONFIGURACIÓN DE BASE DE DATOS")
    print("-" * 40)
    
    try:
        create_all_tables()
        print("✅ Tablas creadas exitosamente")
        
        # Verificar creación
        check_database()
        
    except Exception as e:
        print(f"❌ Error configurando base de datos: {e}")

def show_recent_data():
    """Muestra datos recientes de la base de datos."""
    print("📊 DATOS RECIENTES")
    print("-" * 40)
    
    try:
        with get_session() as session:
            # Últimos 10 resultados de qualifying
            results = session.query(
                QualifyingResult.season,
                QualifyingResult.round,
                Driver.code,
                Driver.given_name,
                Driver.family_name,
                Constructor.name,
                QualifyingResult.position
            ).join(
                Driver, QualifyingResult.driver_db_id == Driver.id
            ).join(
                Constructor, QualifyingResult.constructor_db_id == Constructor.id
            ).order_by(
                QualifyingResult.season.desc(),
                QualifyingResult.round.desc(),
                QualifyingResult.position.asc()
            ).limit(10).all()
            
            if results:
                print("Últimos resultados de qualifying:")
                print(f"{'Temporada':<10} {'Ronda':<6} {'Código':<6} {'Piloto':<25} {'Constructor':<15} {'Pos':<4}")
                print("-" * 70)
                
                for result in results:
                    season, round_num, code, given_name, family_name, constructor, position = result
                    full_name = f"{given_name} {family_name}"
                    print(f"{season:<10} {round_num:<6} {code or 'N/A':<6} {full_name:<25} {constructor:<15} {position or 'N/A':<4}")
            else:
                print("No hay datos en la base de datos")
                
    except Exception as e:
        print(f"❌ Error mostrando datos: {e}")

def generate_migration():
    """Genera una nueva migración con Alembic."""
    message = input("Ingresa el mensaje para la migración: ").strip()
    if not message:
        message = f"Migration_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    
    print(f"🔄 Generando migración: {message}")
    
    # Ejecutar alembic revision
    import subprocess
    try:
        result = subprocess.run([
            sys.executable, "-m", "alembic", "revision", "--autogenerate", "-m", message
        ], cwd=os.path.dirname(__file__), capture_output=True, text=True)
        
        if result.returncode == 0:
            print("✅ Migración generada exitosamente")
            print(result.stdout)
        else:
            print("❌ Error generando migración")
            print(result.stderr)
    except Exception as e:
        print(f"❌ Error ejecutando alembic: {e}")

def apply_migrations():
    """Aplica todas las migraciones pendientes."""
    print("🚀 Aplicando migraciones...")
    
    import subprocess
    try:
        result = subprocess.run([
            sys.executable, "-m", "alembic", "upgrade", "head"
        ], cwd=os.path.dirname(__file__), capture_output=True, text=True)
        
        if result.returncode == 0:
            print("✅ Migraciones aplicadas exitosamente")
            print(result.stdout)
        else:
            print("❌ Error aplicando migraciones")
            print(result.stderr)
    except Exception as e:
        print(f"❌ Error ejecutando alembic: {e}")

def show_config():
    """Muestra la configuración actual del proyecto."""
    print("⚙️  CONFIGURACIÓN DEL PROYECTO")
    print("-" * 40)
    
    print(f"Entorno: {Config.ENVIRONMENT}")
    print(f"Debug: {Config.DEBUG}")
    print(f"URL Base de datos: {Config.get_database_url()}")
    print(f"Echo SQL: {Config.SQLALCHEMY_ECHO}")
    print(f"Pool size: {Config.SQLALCHEMY_POOL_SIZE}")
    print(f"Max overflow: {Config.SQLALCHEMY_MAX_OVERFLOW}")
    print(f"Archivo CSV: {Config.INPUT_PATH}")
    print(f"SQLite (legacy): {Config.SQLITE_DB_PATH}")
    
    if Config.SUPABASE_URL:
        print(f"Supabase URL: {Config.SUPABASE_URL}")
        print(f"Supabase Key: {'***' + Config.SUPABASE_ANON_KEY[-4:] if Config.SUPABASE_ANON_KEY else 'No configurada'}")

def main():
    """Función principal con argumentos de línea de comandos."""
    parser = argparse.ArgumentParser(description='Utilidades para el proyecto ETL F1')
    parser.add_argument('command', 
                       choices=['check-db', 'setup-db', 'show-data', 'config', 'migrate', 'upgrade'],
                       help='Comando a ejecutar')
    
    args = parser.parse_args()
    
    print_banner()
    
    if args.command == 'check-db':
        check_database()
    elif args.command == 'setup-db':
        setup_database()
    elif args.command == 'show-data':
        show_recent_data()
    elif args.command == 'config':
        show_config()
    elif args.command == 'migrate':
        generate_migration()
    elif args.command == 'upgrade':
        apply_migrations()

if __name__ == "__main__":
    if len(sys.argv) == 1:
        print_banner()
        print("Comandos disponibles:")
        print("  check-db    - Verificar estado de la base de datos")
        print("  setup-db    - Configurar base de datos (crear tablas)")
        print("  show-data   - Mostrar datos recientes")
        print("  config      - Mostrar configuración actual")
        print("  migrate     - Generar nueva migración")
        print("  upgrade     - Aplicar migraciones pendientes")
        print("\nEjemplo: python utils.py check-db")
    else:
        main()
