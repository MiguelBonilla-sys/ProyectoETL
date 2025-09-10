# 🏎️ ETL Project - Procesamiento de Datos F1 Qualifying

## 📋 Descripción del Proyecto

Este proyecto implementa un pipeline ETL (Extract, Transform, Load) completo para procesar datos de qualifying de Fórmula 1. El sistema extrae datos de archivos CSV, los transforma y limpia (incluyendo la generación automática de códigos de pilotos), y los carga en diferentes formatos de salida.

### 🎯 Características Principales

- **Extracción** de datos desde archivos CSV
- **Transformación** y limpieza de datos con generación automática de códigos
- **Carga** a múltiples destinos (CSV y SQLite)
- **Interfaz de línea de comandos** intuitiva
- **Logging** detallado del proceso
- **Manejo robusto de errores**

## 🏗️ Arquitectura del Proyecto

```
ETLProject/
├── main.py                    # Punto de entrada principal
├── Config/
│   └── config.py             # Configuraciones del proyecto
├── Extract/
│   ├── extractor.py          # Módulo de extracción
│   └── Files/
│       └── qualifying_results.csv  # Datos fuente
├── Transform/
│   └── transformer.py        # Módulo de transformación
├── Load/
│   └── loader.py             # Módulo de carga
├── Output/                   # Directorio de archivos generados
├── test_qualifying_transformer.py  # Script de pruebas
├── README.md                 # Documentación del proyecto
└── LICENSE                   # Licencia del proyecto
```

## 🚀 Instalación y Configuración

### Prerrequisitos

- Python 3.7 o superior
- pandas
- sqlite3 (incluido en Python)

### Instalación

1. **Clonar el repositorio:**
   ```bash
   git clone https://github.com/MiguelBonilla-sys/ETLProject.git
   cd ETLProject
   ```

2. **Crear entorno virtual (recomendado):**
   ```bash
   python -m venv .venv
   .venv\Scripts\activate  # Windows
   # source .venv/bin/activate  # Linux/Mac
   ```

3. **Instalar dependencias:**
   ```bash
   pip install pandas
   ```

## 🔧 Uso del Sistema

### Ejecución Principal

```bash
# Ejecutar el pipeline ETL completo
python main.py
```

### Comandos Adicionales

```bash
# Ver muestra de datos procesados
python main.py show

# Mostrar ayuda
python main.py help

# Ejecutar pruebas del transformador
python test_qualifying_transformer.py
```

## 📊 Descripción de los Módulos

### 1. 📥 Extract (Extracción)

**Archivo:** `Extract/extractor.py`

**Funcionalidad:**
- Extrae datos desde archivos CSV
- Manejo de errores de lectura
- Soporte para diferentes rutas de archivo

**Métodos principales:**
- `extract()`: Extrae datos del archivo configurado
- `extract_csv(file_path)`: Extrae datos de un archivo específico

### 2. 🔄 Transform (Transformación)

**Archivo:** `Transform/transformer.py`

**Funcionalidad:**
- Limpieza y transformación de datos
- Generación automática de códigos de pilotos
- Conversión de tipos de datos
- Manejo de valores nulos

**Métodos principales:**
- `clean()`: Limpieza general de datos
- `clean_qualifying_data()`: Limpieza específica para datos de qualifying

**Transformaciones de qualifying:**
- ✅ Genera campo `Code` con las primeras 3 letras del `FamilyName`
- ✅ Limpia espacios en blanco en columnas de texto
- ✅ Convierte columnas numéricas apropiadamente
- ✅ Procesa fechas de nacimiento
- ✅ Mantiene formato de tiempos de qualifying

### 3. 💾 Load (Carga)

**Archivo:** `Load/loader.py`

**Funcionalidad:**
- Carga datos a archivos CSV
- Carga datos a base de datos SQLite
- Creación automática de directorios
- Manejo de errores con fallback

**Métodos principales:**
- `to_csv(output_path)`: Guarda en formato CSV
- `to_sqlite(db_path, table_name)`: Guarda en SQLite

### 4. ⚙️ Config (Configuración)

**Archivo:** `Config/config.py`

**Parámetros configurables:**
- `INPUT_PATH`: Ruta del archivo de datos fuente
- `SQLITE_DB_PATH`: Ruta de la base de datos SQLite
- `SQLITE_TABLE`: Nombre de la tabla en SQLite

## 📈 Datos de Entrada

### Formato del CSV de Qualifying

El archivo `qualifying_results.csv` contiene las siguientes columnas:

| Columna | Descripción | Tipo |
|---------|-------------|------|
| Season | Temporada | int |
| Round | Ronda del campeonato | int |
| CircuitID | Identificador del circuito | string |
| Position | Posición en qualifying | int |
| DriverID | Identificador del piloto | string |
| Code | Código del piloto (se genera automáticamente) | string |
| PermanentNumber | Número permanente del piloto | int |
| GivenName | Nombre del piloto | string |
| FamilyName | Apellido del piloto | string |
| DateOfBirth | Fecha de nacimiento | date |
| Nationality | Nacionalidad | string |
| ConstructorID | Identificador del constructor | string |
| ConstructorName | Nombre del constructor | string |
| ConstructorNationality | Nacionalidad del constructor | string |
| Q1, Q2, Q3 | Tiempos de qualifying | string |

## 📊 Datos de Salida

### 1. Archivo CSV Limpio
- **Ubicación:** `Output/qualifying_results_clean_YYYYMMDD_HHMMSS.csv`
- **Formato:** CSV con timestamp
- **Contenido:** Datos limpios y transformados

### 2. Base de Datos SQLite
- **Ubicación:** `Extract/Files/f1_data.db`
- **Tabla:** `qualifying_results`
- **Contenido:** Datos limpios en formato relacional

## 🔍 Ejemplos de Transformación

### Generación de Códigos de Pilotos

```python
# Ejemplos de códigos generados automáticamente:
Häkkinen    → HÄK
Coulthard   → COU
Schumacher  → SCH
Barrichello → BAR
Frentzen    → FRE
```

### Estadísticas de Ejemplo

```
📈 Estadísticas finales:
   • Registros procesados: 8,918
   • Pilotos únicos: 122
   • Constructores únicos: 38
   • Temporadas: 2000 - 2024
   • Códigos generados: 109
```

## 🧪 Testing

### Ejecutar Pruebas

```bash
python test_qualifying_transformer.py
```

### Resultados Esperados

```
✅ Datos extraídos: 8918 registros
✅ Datos transformados exitosamente
✅ Códigos únicos generados: 109
✅ Códigos vacíos: 0
✅ Prueba completada exitosamente!
```

## 🛠️ Desarrollo y Mantenimiento

### Estructura de Clases

```python
# Extractor
class Extractor:
    def __init__(self, file_path=None)
    def extract()
    def extract_csv(file_path)

# Transformer
class Transformer:
    def __init__(self, df)
    def clean()
    def clean_qualifying_data()

# Loader
class Loader:
    def __init__(self, df)
    def to_csv(output_path)
    def to_sqlite(db_path, table_name)
```

### Agregar Nuevas Transformaciones

1. Agregar método en `Transformer` class
2. Actualizar `main.py` para usar el nuevo método
3. Agregar pruebas correspondientes

### Agregar Nuevos Destinos de Carga

1. Agregar método en `Loader` class
2. Actualizar configuración si es necesario
3. Actualizar `main.py` para incluir el nuevo destino

## 🚨 Manejo de Errores

### Errores Comunes y Soluciones

| Error | Causa | Solución |
|-------|-------|----------|
| `FileNotFoundError` | Archivo CSV no encontrado | Verificar ruta en `config.py` |
| `PermissionError` | Sin permisos de escritura | Verificar permisos del directorio |
| `sqlite3.OperationalError` | Error de base de datos | Verificar espacio en disco |
| `pandas.errors.EmptyDataError` | Archivo CSV vacío | Verificar contenido del archivo |

### Logs y Debugging

El sistema proporciona logging detallado:
- ✅ Mensajes de éxito
- ❌ Mensajes de error
- ℹ️ Información general
- 📊 Estadísticas de procesamiento

## 📄 Licencia

Este proyecto está licenciado bajo la MIT License. Ver el archivo `LICENSE` para más detalles.

## 👤 Autor

**Miguel Bonilla**
- GitHub: [@MiguelBonilla-sys](https://github.com/MiguelBonilla-sys)
- Proyecto: [ETLProject](https://github.com/afmirandad/ETLProject) fuente de aprendizaje

---

*Proyecto desarrollado como parte del aprendizaje de pipelines ETL y procesamiento de datos de Fórmula 1.*