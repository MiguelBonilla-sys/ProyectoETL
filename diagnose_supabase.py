#!/usr/bin/env python3
"""
Script simple para diagnosticar la conexión a Supabase
"""

import os
import sys
from datetime import datetime

# Configurar path para imports locales
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

try:
    print("="*60)
    print("🔧 DIAGNÓSTICO DE CONEXIÓN SUPABASE")
    print("="*60)
    print(f"Iniciado: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("-"*60)
    
    # Test 1: Importar módulos
    print("📦 Test 1: Importando módulos...")
    try:
        from Database.connection import DatabaseManager, get_session
        print("  ✅ Database.connection importado correctamente")
    except Exception as e:
        print(f"  ❌ Error importando Database.connection: {e}")
        sys.exit(1)
    
    try:
        from Models.driver import Driver
        from Models.constructor import Constructor
        from Models.qualifying import QualifyingResult
        print("  ✅ Modelos importados correctamente")
    except Exception as e:
        print(f"  ❌ Error importando modelos: {e}")
        sys.exit(1)
    
    # Test 2: Verificar variables de entorno
    print("\n🔧 Test 2: Verificando configuración...")
    database_url = os.getenv('DATABASE_URL')
    if database_url:
        # Ocultar la contraseña en el log
        safe_url = database_url.replace('vAdb2QmOR3BhmNew', '***PASSWORD***')
        print(f"  ✅ DATABASE_URL configurada: {safe_url}")
    else:
        print("  ❌ DATABASE_URL no configurada")
        sys.exit(1)
    
    # Test 3: Probar conexión
    print("\n🌐 Test 3: Probando conexión a Supabase...")
    try:
        from Database.connection import get_engine
        engine = get_engine()
        print("  ✅ Engine obtenido correctamente")
        
        # Probar conexión real
        with engine.connect() as conn:
            from sqlalchemy import text
            result = conn.execute(text("SELECT 1 as test"))
            test_value = result.fetchone()[0]
            if test_value == 1:
                print("  ✅ Conexión a Supabase exitosa")
            else:
                print("  ❌ Conexión problemática")
                
    except Exception as e:
        print(f"  ❌ Error de conexión: {e}")
        sys.exit(1)
    
    # Test 4: Verificar tablas
    print("\n📋 Test 4: Verificando tablas en Supabase...")
    try:
        with get_session() as session:
            from sqlalchemy import text
            # Verificar si las tablas existen usando queries simples
            try:
                driver_count = session.execute(text("SELECT COUNT(*) FROM drivers")).scalar()
                print(f"  ✅ Tabla 'drivers' existe - Registros: {driver_count}")
            except Exception as e:
                print(f"  ❌ Tabla 'drivers' no existe o error: {e}")
            
            try:
                constructor_count = session.execute(text("SELECT COUNT(*) FROM constructors")).scalar()
                print(f"  ✅ Tabla 'constructors' existe - Registros: {constructor_count}")
            except Exception as e:
                print(f"  ❌ Tabla 'constructors' no existe o error: {e}")
            
            try:
                qualifying_count = session.execute(text("SELECT COUNT(*) FROM qualifying_results")).scalar()
                print(f"  ✅ Tabla 'qualifying_results' existe - Registros: {qualifying_count}")
            except Exception as e:
                print(f"  ❌ Tabla 'qualifying_results' no existe o error: {e}")
                
    except Exception as e:
        print(f"  ❌ Error verificando tablas: {e}")
    
    print("\n" + "="*60)
    print("✅ DIAGNÓSTICO COMPLETADO")
    print("="*60)
    
except Exception as e:
    print(f"❌ Error general en diagnóstico: {e}")
    import traceback
    traceback.print_exc()
