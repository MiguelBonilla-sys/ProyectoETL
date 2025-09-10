#!/usr/bin/env python3
"""
Demo ETL para SQLite
====================

Demostración del pipeline ETL usando SQLite directamente.

Autor: Miguel Bonilla
Fecha: Septiembre 2025
"""

import sys
import os
from datetime import datetime

import os
import sys
from datetime import datetime

# Configurar path para imports locales
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Remover cualquier configuración de Supabase
if 'DATABASE_URL' in os.environ:
    del os.environ['DATABASE_URL']
os.environ['ENVIRONMENT'] = 'development'

def demo_etl_sqlite():
    """Demo completo del ETL con SQLite"""
    
    print("="*70)
    print("🏎️  DEMO: ETL F1 con SQLAlchemy + SQLite")
    print("="*70)
    print(f"Iniciado: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("-"*70)
    
    try:
        # Importar módulos
        from Extract.F1Extract import Extractor
        from Transform.F1Transformer import Transformer
        from Load.F1Loader import Loader
        
        # Configuración manual para SQLite
        input_path = "Extract/Files/qualifying_results.csv"
        sqlite_db = "f1_demo.db"
        
        print(f"📂 Archivo entrada: {input_path}")
        print(f"📂 Base de datos: {sqlite_db}")
        print("-"*70)
        
        # 1. EXTRACT
        print("📥 EXTRAYENDO DATOS...")
        extractor = Extractor()
        df_raw = extractor.extract_csv(input_path)
        print(f"   ✅ {len(df_raw):,} registros extraídos")
        
        # 2. TRANSFORM
        print("\n🔄 TRANSFORMANDO DATOS...")
        transformer = Transformer(df_raw)
        drivers, constructors, qualifying_results = transformer.transform_to_orm_objects()
        
        print(f"   ✅ {len(drivers)} pilotos únicos")
        print(f"   ✅ {len(constructors)} constructores únicos")
        print(f"   ✅ {len(qualifying_results)} resultados qualifying")
        
        # 3. LOAD
        print("\n📤 CARGANDO A SQLITE...")
        
        # Configurar conexión SQLite manualmente
        from sqlalchemy import create_engine
        from sqlalchemy.orm import sessionmaker
        from Models.driver import Driver
        from Models.constructor import Constructor
        from Models.qualifying import QualifyingResult
        
        # Eliminar la base de datos existente si existe
        if os.path.exists(sqlite_db):
            os.remove(sqlite_db)
            print("   🗑️ Base de datos anterior eliminada")
        
        # Engine SQLite
        sqlite_url = f"sqlite:///{sqlite_db}"
        engine = create_engine(sqlite_url, echo=False)
        
        # Crear tablas
        from Models.base import BaseModel
        BaseModel.metadata.create_all(engine)
        print("   ✅ Tablas creadas")
        
        # Sesión
        session_factory = sessionmaker(bind=engine)
        session = session_factory()
        
        try:
            # Cargar pilotos
            print(f"   📋 Cargando {len(drivers)} pilotos...")
            driver_map = {}
            for driver in drivers:
                # Buscar si ya existe
                existing_driver = session.query(Driver).filter_by(driver_id=driver.driver_id).first()
                if existing_driver:
                    driver_map[driver.driver_id] = existing_driver
                else:
                    session.add(driver)
                    session.flush()  # Para obtener el ID
                    driver_map[driver.driver_id] = driver
            
            # Cargar constructores
            print(f"   📋 Cargando {len(constructors)} constructores...")
            constructor_map = {}
            for constructor in constructors:
                # Buscar si ya existe
                existing_constructor = session.query(Constructor).filter_by(constructor_id=constructor.constructor_id).first()
                if existing_constructor:
                    constructor_map[constructor.constructor_id] = existing_constructor
                else:
                    session.add(constructor)
                    session.flush()  # Para obtener el ID
                    constructor_map[constructor.constructor_id] = constructor
            
            # Commit parcial
            session.commit()
            print("   ✅ Pilotos y constructores cargados")
            
            # Cargar resultados qualifying con relaciones correctas
            print(f"   📋 Cargando {len(qualifying_results)} resultados...")
            
            for result in qualifying_results:
                # Establecer relaciones usando los IDs guardados
                driver_id = getattr(result, '_driver_id', None)
                constructor_id = getattr(result, '_constructor_id', None)
                
                if driver_id in driver_map and constructor_id in constructor_map:
                    result.driver_db_id = driver_map[driver_id].id
                    result.constructor_db_id = constructor_map[constructor_id].id
                    session.merge(result)
                else:
                    print(f"   ⚠️ Saltando resultado: driver_id={driver_id}, constructor_id={constructor_id}")
            
            session.commit()
            
            print("   ✅ Todos los datos cargados")
            
            # 4. VERIFICACIÓN
            print("\n📋 VERIFICANDO DATOS...")
            
            driver_count = session.query(Driver).count()
            constructor_count = session.query(Constructor).count()
            qualifying_count = session.query(QualifyingResult).count()
            
            print(f"   ✅ Pilotos en BD: {driver_count:,}")
            print(f"   ✅ Constructores en BD: {constructor_count:,}")
            print(f"   ✅ Resultados en BD: {qualifying_count:,}")
            
            # Ejemplos
            print("\n📋 EJEMPLOS DE DATOS:")
            
            # Piloto ejemplo
            sample_driver = session.query(Driver).first()
            if sample_driver:
                print(f"   👨‍🏎️ Piloto: {sample_driver.full_name} ({sample_driver.nationality})")
            
            # Constructor ejemplo
            sample_constructor = session.query(Constructor).first()
            if sample_constructor:
                print(f"   🏭 Constructor: {sample_constructor.name} ({sample_constructor.nationality})")
            
            # Resultado ejemplo
            sample_result = session.query(QualifyingResult).first()
            if sample_result:
                print(f"   🏁 Resultado: Pos {sample_result.position} - Q1: {sample_result.q1}")
            
            session.close()
            
            print("\n" + "="*70)
            print("🏆 ¡DEMO ETL COMPLETADO EXITOSAMENTE!")
            print(f"📁 Base de datos creada: {sqlite_db}")
            print("="*70)
            return True
            
        except Exception as e:
            session.rollback()
            session.close()
            raise e
            
    except Exception as e:
        print(f"❌ Error en demo: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = demo_etl_sqlite()
    
    if success:
        print("\n🎉 ¡Sistema ETL con SQLAlchemy funcionando perfectamente!")
        print("💡 Puedes inspeccionar la base de datos f1_demo.db")
    else:
        print("\n💥 Error en el demo ETL")
