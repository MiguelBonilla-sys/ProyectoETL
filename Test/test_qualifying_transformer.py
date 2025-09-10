#!/usr/bin/env python3
"""
Script de prueba para el transformador de datos de qualifying.
"""

import pandas as pd
import sys
import os

# Agregar el directorio padre al path para poder importar los módulos
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from Extract.F1Extract import Extractor
from Transform.F1Transformer import Transformer

def test_qualifying_transformer():
    """
    Función para probar el transformador de datos de qualifying.
    """
    try:
        # Cargar los datos de qualifying
        extractor = Extractor()
        df_qualifying = extractor.extract_csv(r'Extract\Files\qualifying_results.csv')
        
        print("Datos originales (primeras 5 filas):")
        print(df_qualifying.head())
        print(f"\nColumnas disponibles: {list(df_qualifying.columns)}")
        
        # Crear el transformador y limpiar los datos
        transformer = Transformer(df_qualifying)
        df_cleaned = transformer.clean_qualifying_data()
        
        print("\n" + "="*50)
        print("Datos después de la transformación (primeras 5 filas):")
        print(df_cleaned.head())
        
        print("\n" + "="*50)
        print("Verificación del campo 'Code' generado:")
        print(df_cleaned[['FamilyName', 'Code']].head(10))
        
        # Verificar casos especiales
        print("\n" + "="*50)
        print("Estadísticas del campo 'Code':")
        print(f"Total de registros: {len(df_cleaned)}")
        print(f"Códigos únicos: {df_cleaned['Code'].nunique()}")
        print(f"Códigos vacíos: {(df_cleaned['Code'] == '').sum()}")
        
        return df_cleaned
        
    except Exception as e:
        print(f"Error durante la prueba: {e}")
        return None

if __name__ == "__main__":
    result = test_qualifying_transformer()
    if result is not None:
        print("\n✅ Prueba completada exitosamente!")
    else:
        print("\n❌ La prueba falló.")
