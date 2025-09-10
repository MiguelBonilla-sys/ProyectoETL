#!/usr/bin/env python3
"""
Script simple para cargar datos a Supabase sin logs complejos
"""

import os
import sys
from datetime import datetime

# Configurar path para imports locales
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

def load_data_to_supabase():
    """Carga simple de datos a Supabase"""
    try:
        print("Iniciando carga a Supabase...")
        
        # 1. Importar módulos necesarios
        from Extract.F1Extract import Extractor
        from Transform.F1Transformer import Transformer
        from Load.F1Loader import Loader
        from Config.config import Config
        
        print("Modulos importados correctamente")
        
        # 2. Extraer datos
        extractor = Extractor()
        df_raw = extractor.extract_csv(Config.INPUT_PATH)
        print(f"Datos extraidos: {len(df_raw)} registros")
        
        # 3. Transformar datos
        transformer = Transformer(df_raw)
        drivers, constructors, qualifying_results = transformer.transform_to_orm_objects()
        print(f"Datos transformados: {len(drivers)} pilotos, {len(constructors)} constructores, {len(qualifying_results)} qualifying")
        
        # 4. Cargar a Supabase
        loader = Loader(drivers, constructors, qualifying_results)
        
        # Crear tablas primero
        from Database.connection import create_all_tables
        create_all_tables()
        print("Tablas creadas/verificadas")
        
        # Verificar los objetos antes de cargar
        print("Verificando primeros objetos:")
        if drivers:
            first_driver = drivers[0]
            print(f"  Driver: {first_driver.driver_id if hasattr(first_driver, 'driver_id') else 'NO driver_id'}")
        if constructors:
            first_constructor = constructors[0]
            print(f"  Constructor: {first_constructor.constructor_id if hasattr(first_constructor, 'constructor_id') else 'NO constructor_id'}")
        if qualifying_results:
            first_result = qualifying_results[0]
            print(f"  Result: race_id={getattr(first_result, 'race_id', 'NO race_id')}")
        
        # Cargar datos usando el método específico de Supabase
        success = loader.to_supabase()
        
        if success:
            print("✅ CARGA A SUPABASE EXITOSA!")
            return True
        else:
            print("❌ Error en la carga a Supabase")
            return False
            
    except Exception as e:
        print(f"❌ Error general: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    print("="*50)
    print("🚀 CARGA SIMPLE A SUPABASE")
    print("="*50)
    
    success = load_data_to_supabase()
    
    if success:
        print("\n🎉 ¡Proceso completado exitosamente!")
    else:
        print("\n💥 Proceso falló")
    
    print("="*50)
