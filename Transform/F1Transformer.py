import pandas as pd
class Transformer:
    """
    Clase para transformar y limpiar los datos extraídos.
    """
    def __init__(self, df):
        self.df = df

    def clean(self):
        """
        Realiza limpieza y transformación de los datos.
        """
        df = self.df.copy()
        # Limpieza de columnas de fecha y hora
        df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
        df['Time'] = pd.to_datetime(df['Time'], errors='coerce').dt.time
        # Eliminar filas con Booking ID nulo
        df = df.dropna(subset=['Booking ID'])
        # Rellenar valores nulos en columnas numéricas con 0
        num_cols = ['Avg VTAT', 'Avg CTAT', 'Booking Value', 'Ride Distance', 'Driver Ratings', 'Customer Rating']
        for col in num_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
        # Rellenar valores nulos en columnas de texto con 'Unknown'
        text_cols = ['Booking Status', 'Vehicle Type', 'Pickup Location', 'Drop Location',
                     'Reason for cancelling by Customer', 'Driver Cancellation Reason',
                     'Incomplete Rides Reason', 'Payment Method']
        for col in text_cols:
            if col in df.columns:
                df[col] = df[col].fillna('Unknown')
        # Convertir flags a booleanos
        flag_cols = ['Cancelled Rides by Customer', 'Cancelled Rides by Driver', 'Incomplete Rides']
        for col in flag_cols:
            if col in df.columns:
                df[col] = df[col].astype(bool)
        self.df = df
        return self.df

    def clean_qualifying_data(self):
        """
        Realiza limpieza específica para los datos de qualifying.
        Genera el campo 'Code' con las primeras 3 letras del 'FamilyName'.
        """
        df = self.df.copy()
        
        # Verificar si existe la columna FamilyName
        if 'FamilyName' in df.columns:
            # Generar el código con las primeras 3 letras del apellido en mayúsculas
            df['Code'] = df['FamilyName'].apply(lambda x: str(x)[:3].upper() if pd.notna(x) and str(x).strip() != '' else '')
            
            # Limpiar espacios en blanco en columnas de texto
            text_columns = ['FamilyName', 'GivenName', 'Nationality', 'ConstructorName', 'ConstructorNationality']
            for col in text_columns:
                if col in df.columns:
                    df[col] = df[col].astype(str).str.strip()
            
            # Convertir columnas de tiempo a formato numérico si es necesario
            time_columns = ['Q1', 'Q2', 'Q3']
            for col in time_columns:
                if col in df.columns:
                    # Mantener como string si ya está en formato de tiempo (mm:ss.sss)
                    # Si son ceros, mantenerlos como están
                    df[col] = df[col].astype(str)
            
            # Convertir columnas numéricas
            numeric_columns = ['Season', 'Round', 'Position', 'PermanentNumber']
            for col in numeric_columns:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
            
            # Limpiar y convertir fecha de nacimiento
            if 'DateOfBirth' in df.columns:
                df['DateOfBirth'] = pd.to_datetime(df['DateOfBirth'], errors='coerce')
        
        self.df = df
        return self.df