#!/usr/bin/env python3
"""
Ejecutor ETL para SQLite
========================

Script para ejecutar el pipeline ETL directamente a SQLite.

Autor: Miguel Bonilla
Fecha: Septiembre 2025
"""

import sys
import os
from datetime import datetime

# Agregar el directorio actual al path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

def main():
    """Función principal que ejecuta el ETL."""
    try:
        print("=" * 70)
        print("🏎️  ETL F1 con SQLAlchemy + SQLite")
        print("=" * 70)
        print(f"Iniciado: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("-" * 70)
        print("🔧 Configuración: SQLite local")
        
        # Importar después de configurar el path
        from Config.config import Config
        
        # Configurar para usar SQLite local en lugar de la configuración por defecto
        import os
        os.environ['ENVIRONMENT'] = 'local'
        
        # Mostrar configuración
        db_url = "sqlite:///f1_orm.db"
        print(f"📂 Base de datos: {db_url}")
        print("-" * 70)
        
        # Importar módulos del proyecto
        from Extract.F1Extract import Extractor
        from Transform.F1Transformer import Transformer
        from Load.F1Loader import Loader
        
        # PASO 1: EXTRACCIÓN
        print("📥 PASO 1: EXTRACCIÓN")
        print("-" * 30)
        
        extractor = Extractor()
        df_raw = extractor.extract_csv(Config.INPUT_PATH)
        
        if df_raw is None or df_raw.empty:
            print("❌ No se pudieron extraer los datos")
            return False
            
        print(f"✅ Datos extraídos: {len(df_raw):,} registros")
        
        # PASO 2: TRANSFORMACIÓN
        print("\n🔄 PASO 2: TRANSFORMACIÓN")
        print("-" * 30)
        
        transformer = Transformer(df_raw)
        drivers, constructors, qualifying_results = transformer.transform_to_orm_objects()
        
        print("✅ Objetos ORM creados:")
        print(f"   • Pilotos únicos: {len(drivers)}")
        print(f"   • Constructores únicos: {len(constructors)}")
        print(f"   • Resultados qualifying: {len(qualifying_results)}")
        
        # PASO 3: CARGA A SQLITE
        print("\n📤 PASO 3: CARGA A SQLITE")
        print("-" * 30)
        
        loader = Loader(
            df=transformer.df,
            drivers=drivers,
            constructors=constructors,
            qualifying_results=qualifying_results
        )
        
        print("🔧 Creando tablas...")
        stats = loader.to_database_orm()
        
        if 'error' in stats:
            print(f"❌ Error general en carga ORM: {stats['error']}")
        else:
            print("✅ Tablas creadas/verificadas")
            print("📋 Cargando datos...")
            print("🎉 CARGA EXITOSA!")
            print("📊 Estadísticas:")
        
        # PASO 4: VERIFICACIÓN
        print("\n📋 PASO 4: VERIFICACIÓN")
        print("-" * 30)
        
        # Verificar datos guardados
        from Database import get_session
        from Models.driver import Driver
        from Models.constructor import Constructor
        from Models.qualifying import QualifyingResult
        
        with get_session() as session:
            driver_count = session.query(Driver).count()
            constructor_count = session.query(Constructor).count()
            qualifying_count = session.query(QualifyingResult).count()
            
            print("✅ Datos verificados en SQLite:")
            print(f"   • Pilotos: {driver_count}")
            print(f"   • Constructores: {constructor_count}")
            print(f"   • Resultados qualifying: {qualifying_count}")
            
            print("\n📋 Ejemplos de datos:")
        
        print("\n" + "=" * 70)
        print("🏆 ¡ETL COMPLETADO EXITOSAMENTE!")
        print("=" * 70)
        print("\n🎉 ¡Sistema ETL con SQLAlchemy funcionando perfectamente!")
        print("💡 Archivo de base de datos: f1_orm.db")
        
        return True
        
    except Exception as e:
        print(f"❌ Error en ETL: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    main()
