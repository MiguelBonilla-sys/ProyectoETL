import pandas as pd

class Extractor:
    """
    Clase para extraer datos de archivos fuente.
    """
    def __init__(self, file_path=None):
        self.file_path = file_path

    def extract(self):
        """
        Extrae los datos del archivo especificado.
        """
        try:
            df = pd.read_csv(self.file_path)
            return df
        except Exception as e:
            print(f"Error al extraer datos: {e}")
            return None

    def extract_csv(self, file_path):
        """
        Extrae datos de un archivo CSV específico.
        """
        try:
            df = pd.read_csv(file_path)
            return df
        except Exception as e:
            print(f"Error al extraer datos de {file_path}: {e}")
            return None