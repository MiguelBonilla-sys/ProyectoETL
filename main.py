
import sys
import os
from datetime import datetime

# Agregar el directorio actual al path para imports
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Imports de módulos del proyecto
from Extract.F1Extract import Extractor
from Transform.F1Transformer import Transformer
from Load.F1Loader import Loader
from Config.config import Config


def print_banner():
    """Imprime el banner del proyecto."""
    print("=" * 60)
    print("🏎️  PROYECTO ETL - DATOS QUALIFYING F1")
    print("=" * 60)
    print(f"Iniciado: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("-" * 60)


def print_step(step_number, description):
    """Imprime información del paso actual."""
    print(f"\n📋 PASO {step_number}: {description}")
    print("-" * 40)


def print_success(message):
    """Imprime mensaje de éxito."""
    print(f"✅ {message}")


def print_error(message):
    """Imprime mensaje de error."""
    print(f"❌ ERROR: {message}")


def print_info(message):
    """Imprime información general."""
    print(f"ℹ️  {message}")


def main():
    """
    Función principal que ejecuta el pipeline ETL completo.
    """
    try:
        print_banner()
        
        # PASO 1: EXTRACCIÓN
        print_step(1, "EXTRACCIÓN DE DATOS")
        print_info(f"Archivo fuente: {Config.INPUT_PATH}")
        
        extractor = Extractor()
        df_raw = extractor.extract_csv(Config.INPUT_PATH)
        
        if df_raw is None:
            print_error("No se pudieron extraer los datos")
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
        
        loader = Loader(df_clean)
        
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
        loader.to_csv(csv_output)
        print_success(f"Archivo CSV guardado: {csv_output}")
        
        
        # RESUMEN FINAL
        print("\n" + "=" * 60)
        print("🎉 PIPELINE ETL COMPLETADO EXITOSAMENTE")
        print("=" * 60)
        print("📈 Estadísticas finales:")
        print(f"   • Registros procesados: {len(df_clean):,}")
        print(f"   • Pilotos únicos: {df_clean['DriverID'].nunique()}")
        print(f"   • Constructores únicos: {df_clean['ConstructorID'].nunique()}")
        print(f"   • Temporadas: {df_clean['Season'].min()} - {df_clean['Season'].max()}")
        print(f"   • Códigos generados: {df_clean['Code'].nunique()}")
        
        print("\n📁 Archivos generados:")
        print(f"   • CSV: {csv_output}")
        
        print(f"\n⏱️  Completado: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        return True
        
    except Exception as e:
        print_error(f"Error inesperado en el pipeline: {e}")
        import traceback
        print(f"Detalles del error:\n{traceback.format_exc()}")
        return False




if __name__ == "__main__":
    """
    Punto de entrada del programa.
    Acepta argumentos de línea de comandos para diferentes operaciones.
    """
    if len(sys.argv) > 1:
        command = sys.argv[1].lower()
        
        if command == "show" or command == "view":
            show_data_sample()
        elif command == "help" or command == "-h":
            print("🏎️  ETL Project - Procesamiento de datos F1")
            print("\nUso:")
            print("  python main.py        - Ejecuta el pipeline ETL completo")
            print("  python main.py show   - Muestra muestra de datos procesados")
            print("  python main.py help   - Muestra esta ayuda")
        else:
            print_error(f"Comando desconocido: {command}")
            print("Usa 'python main.py help' para ver comandos disponibles")
    else:
        # Ejecutar pipeline ETL por defecto
        success = main()
        sys.exit(0 if success else 1)