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
        
        # Configurar SQLite directamente
        from sqlalchemy import create_engine
        from sqlalchemy.orm import sessionmaker
        from Models.base import BaseModel
        from Models.driver import Driver
        from Models.constructor import Constructor
        from Models.qualifying import QualifyingResult
        
        # Eliminar base de datos anterior si existe
        sqlite_db = "f1_orm.db"
        if os.path.exists(sqlite_db):
            os.remove(sqlite_db)
            print("�️ Base de datos anterior eliminada")
        
        # Engine SQLite
        sqlite_url = f"sqlite:///{sqlite_db}"
        engine = create_engine(sqlite_url, echo=False)
        
        # Crear tablas
        BaseModel.metadata.create_all(engine)
        print("✅ Tablas creadas/verificadas")
        
        # Sesión
        Session = sessionmaker(bind=engine)
        session = Session()
        
        try:
            print("📋 Cargando datos...")
            
            # Cargar pilotos
            driver_map = {}
            for driver in drivers:
                merged_driver = session.merge(driver)
                driver_map[driver.driver_id] = merged_driver
            
            # Cargar constructores
            constructor_map = {}
            for constructor in constructors:
                merged_constructor = session.merge(constructor)
                constructor_map[constructor.constructor_id] = merged_constructor
            
            # Commit pilotos y constructores
            session.commit()
            
            # Cargar resultados qualifying con relaciones correctas
            for result in qualifying_results:
                # Establecer relaciones usando los IDs guardados
                driver_id = getattr(result, '_driver_id', None)
                constructor_id = getattr(result, '_constructor_id', None)
                
                if driver_id in driver_map and constructor_id in constructor_map:
                    result.driver_db_id = driver_map[driver_id].id
                    result.constructor_db_id = constructor_map[constructor_id].id
                    session.merge(result)
            
            session.commit()
            session.close()
            
            print("🎉 CARGA EXITOSA!")
            print("📊 Estadísticas:")
            
        except Exception as e:
            session.rollback()
            session.close()
            print(f"❌ Error general en carga ORM: {e}")
            return False
        
        # PASO 4: VERIFICACIÓN
        print("\n📋 PASO 4: VERIFICACIÓN")
        print("-" * 30)
        
        # Verificar datos guardados usando la misma conexión SQLite
        Session = sessionmaker(bind=engine)
        session = Session()
        
        try:
            driver_count = session.query(Driver).count()
            constructor_count = session.query(Constructor).count()
            qualifying_count = session.query(QualifyingResult).count()
            
            print("✅ Datos verificados en SQLite:")
            print(f"   • Pilotos: {driver_count}")
            print(f"   • Constructores: {constructor_count}")
            print(f"   • Resultados qualifying: {qualifying_count}")
            
            # Ejemplos
            sample_driver = session.query(Driver).first()
            sample_constructor = session.query(Constructor).first()
            sample_result = session.query(QualifyingResult).first()
            
            print("\n📋 Ejemplos de datos:")
            if sample_driver:
                print(f"   👨‍🏎️ Piloto: {sample_driver.full_name} ({sample_driver.nationality})")
            if sample_constructor:
                print(f"   🏭 Constructor: {sample_constructor.name} ({sample_constructor.nationality})")
            if sample_result:
                print(f"   🏁 Resultado: Pos {sample_result.position} - Q1: {sample_result.q1}")
                
            session.close()
            
        except Exception as e:
            print(f"❌ Error en verificación: {e}")
            session.close()
        
        print("\n" + "=" * 70)
        print("🏆 ¡ETL COMPLETADO EXITOSAMENTE!")
        print("=" * 70)
        print("\n🎉 ¡Sistema ETL con SQLAlchemy funcionando perfectamente!")
        print(f"💡 Archivo de base de datos: {sqlite_db}")
        
        return True
        
    except Exception as e:
        print(f"❌ Error en ETL: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    main()
