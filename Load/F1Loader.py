
from Config.config import Config
import sqlite3

class Loader:
    """
    Clase para cargar los datos limpios a un destino.
    """
    def __init__(self, df):
        self.df = df

    def to_csv(self, output_path):
        """
        Guarda el DataFrame limpio en un archivo CSV.
        """
        try:
            self.df.to_csv(output_path, index=False)
            print(f"Datos guardados en {output_path}")
        except Exception as e:
            print(f"Error al guardar datos: {e}")

    def to_sqlite(self, db_path=None, table_name=None):
        """
        Guarda el DataFrame limpio en una base de datos SQLite.
        """
        import os
        
        db_path = db_path or Config.SQLITE_DB_PATH
        table_name = table_name or Config.SQLITE_TABLE
        
        try:
            # Crear directorio si no existe
            db_dir = os.path.dirname(db_path)
            if db_dir and not os.path.exists(db_dir):
                os.makedirs(db_dir)
                print(f"Directorio creado para BD: {db_dir}")
            
            # Verificar que el directorio existe y es escribible
            if not os.path.exists(db_dir):
                raise OSError(f"No se puede crear el directorio: {db_dir}")
            
            # Crear conexión a la base de datos
            conn = sqlite3.connect(db_path)
            self.df.to_sql(table_name, conn, if_exists='replace', index=False)
            conn.close()
            print(f"Datos guardados en la base de datos SQLite: {db_path}, tabla: {table_name}")
            
            # Verificar que el archivo se creó correctamente
            if os.path.exists(db_path):
                size = os.path.getsize(db_path)
                print(f"Archivo DB creado exitosamente. Tamaño: {size:,} bytes")
            else:
                print("Advertencia: El archivo de base de datos no se encontró después de la creación")
                
        except Exception as e:
            print(f"Error al guardar en SQLite: {e}")
            print(f"Ruta intentada: {db_path}")
            print(f"Directorio padre: {os.path.dirname(db_path)}")
            
            # Intentar guardar en directorio actual como fallback
            fallback_path = f"f1_data_{table_name}.db"
            try:
                print(f"Intentando guardar en directorio actual: {fallback_path}")
                conn = sqlite3.connect(fallback_path)
                self.df.to_sql(table_name, conn, if_exists='replace', index=False)
                conn.close()
                print(f"✅ Datos guardados exitosamente en: {fallback_path}")
            except Exception as fallback_error:
                print(f"❌ Error en fallback: {fallback_error}")