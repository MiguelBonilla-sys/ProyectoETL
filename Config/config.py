class Config:
    """
    Clase de configuración para rutas y parámetros del ETL.
    """
    INPUT_PATH = r'Extract\Files\qualifying_results.csv'
    SQLITE_DB_PATH = r'Extract\Files\f1_data.db'
    SQLITE_TABLE = 'qualifying_results'