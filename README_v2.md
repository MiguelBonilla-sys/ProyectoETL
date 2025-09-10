# 🏎️ Proyecto ETL F1 2.0 - SQLAlchemy Edition

Proyecto ETL (Extract, Transform, Load) para procesar datos de qualifying de Fórmula 1 con SQLAlchemy, soporte para múltiples bases de datos y capacidades avanzadas.

## 🚀 Características Principales

### ✨ Versión 2.0 - Nuevas Funcionalidades
- **SQLAlchemy ORM**: Modelos estructurados para Driver, Constructor y QualifyingResult
- **Múltiples BD**: Soporte para SQLite, PostgreSQL (Supabase), y más
- **Migraciones**: Control de versiones de esquema con Alembic
- **Validación de datos**: Reportes de calidad automáticos
- **Carga masiva**: Procesamiento eficiente en lotes
- **Configuración flexible**: Variables de entorno y múltiples entornos
- **Logging avanzado**: Registro detallado de operaciones

### 🔧 Funcionalidades Legacy (Compatibles)
- Extracción desde archivos CSV
- Transformación y limpieza de datos
- Carga a archivos CSV y SQLite tradicional
- Generación automática de códigos de piloto

## 📁 Estructura del Proyecto

```
ProyectoETL/
├── Config/                 # Configuración del proyecto
│   ├── config.py          # Configuración principal con SQLAlchemy
│   └── __init__.py
├── Database/              # Gestión de base de datos
│   ├── connection.py      # Engine y sesiones SQLAlchemy
│   ├── migrations/        # Migraciones Alembic
│   └── __init__.py
├── Models/                # Modelos SQLAlchemy ORM
│   ├── base.py           # Modelo base
│   ├── driver.py         # Modelo Piloto
│   ├── constructor.py    # Modelo Constructor
│   ├── qualifying.py     # Modelo QualifyingResult
│   └── __init__.py
├── Extract/               # Extracción de datos
│   ├── F1Extract.py      # Extractor v2.0 con SQLAlchemy
│   └── Files/
│       └── qualifying_results.csv
├── Transform/             # Transformación de datos
│   └── F1Transformer.py  # Transformador v2.0 con ORM
├── Load/                  # Carga de datos
│   └── F1Loader.py       # Loader v2.0 con múltiples destinos
├── Output/                # Archivos de salida
├── Test/                  # Pruebas unitarias
├── main.py               # Script principal original
├── main_v2.py            # Script principal v2.0 SQLAlchemy
├── utils.py              # Utilidades del proyecto
├── alembic.ini           # Configuración Alembic
├── .env                  # Variables de entorno (desarrollo)
├── .env.example          # Plantilla de variables de entorno
└── requirements.txt      # Dependencias del proyecto
```

## 🛠️ Instalación y Configuración

### 1. Requisitos Previos
- Python 3.8+
- Entorno virtual recomendado

### 2. Instalación
```bash
# Clonar el repositorio
git clone <repository-url>
cd ProyectoETL

# Crear entorno virtual
python -m venv .venv

# Activar entorno virtual (Windows)
.venv\Scripts\activate

# Instalar dependencias
pip install -r requirements.txt
```

### 3. Configuración

#### Variables de Entorno
Copiar `.env.example` a `.env` y configurar:

```env
# Desarrollo local con SQLite
DATABASE_URL=sqlite:///Extract/Files/f1_data.db
ENVIRONMENT=development
DEBUG=True

# Para Supabase (PostgreSQL)
# DATABASE_URL=postgresql://postgres:[PASSWORD]@db.[PROJECT-REF].supabase.co:5432/postgres
# SUPABASE_URL=https://[PROJECT-REF].supabase.co
# SUPABASE_ANON_KEY=[YOUR-ANON-KEY]
```

#### Configuración de Supabase
1. Crear proyecto en [Supabase](https://supabase.com)
2. Obtener credenciales del proyecto
3. Actualizar `.env` con los valores de Supabase
4. Las tablas se crearán automáticamente

## 🎯 Uso del Proyecto

### Script Principal v2.0 (Recomendado)

```bash
# Pipeline completo con SQLAlchemy (por defecto)
python main_v2.py

# Pipeline tradicional (compatibilidad)
python main_v2.py traditional

# Carga específica a Supabase
python main_v2.py supabase

# Ver datos procesados
python main_v2.py show

# Crear tablas en BD
python main_v2.py create-tables

# Ayuda
python main_v2.py help
```

### Utilidades del Proyecto

```bash
# Verificar estado de la BD
python utils.py check-db

# Configurar BD (crear tablas)
python utils.py setup-db

# Mostrar datos recientes
python utils.py show-data

# Ver configuración actual
python utils.py config

# Generar migración
python utils.py migrate

# Aplicar migraciones
python utils.py upgrade
```

### Script Original (Legacy)

```bash
# Pipeline tradicional
python main.py

# Ver datos procesados
python main.py show
```

## 🗃️ Modelos de Datos

### Driver (Piloto)
- `driver_id`: Identificador único del piloto
- `permanent_number`: Número permanente del piloto
- `code`: Código de 3 letras (generado automáticamente)
- `given_name`, `family_name`: Nombre y apellido
- `date_of_birth`: Fecha de nacimiento
- `nationality`: Nacionalidad

### Constructor (Equipo)
- `constructor_id`: Identificador único del constructor
- `name`: Nombre del constructor
- `nationality`: Nacionalidad

### QualifyingResult
- `season`, `round`: Temporada y ronda
- `circuit_id`: Identificador del circuito
- `position`: Posición en qualifying
- `q1`, `q2`, `q3`: Tiempos de qualifying
- Relaciones con Driver y Constructor

## 🔄 Pipeline ETL

### 1. Extract (Extracción)
- **CSV**: Archivos locales de qualifying
- **Base de datos**: Consultas SQLAlchemy avanzadas
- **Filtros**: Por temporada, límites, etc.

### 2. Transform (Transformación)
- **Limpieza**: Espacios, valores nulos, tipos de datos
- **Validación**: Reportes de calidad automáticos
- **ORM**: Conversión a objetos SQLAlchemy
- **Códigos**: Generación automática de códigos de piloto

### 3. Load (Carga)
- **CSV**: Archivos de salida estructurados
- **SQLite**: Base de datos local (legacy)
- **PostgreSQL**: Supabase y otros
- **Transacciones**: Rollback automático en errores
- **Batch**: Procesamiento en lotes para grandes volúmenes

## 📊 Características Avanzadas

### Validación de Calidad de Datos
- Conteo de registros y valores nulos
- Detección de duplicados
- Validación de códigos de piloto
- Análisis de tiempos de qualifying
- Reportes estadísticos automáticos

### Gestión de Migraciones
```bash
# Generar migración automática
python utils.py migrate

# Aplicar migraciones pendientes
python utils.py upgrade

# Ver estado de migraciones
alembic current

# Historial de migraciones
alembic history
```

### Logging Avanzado
- Logs en archivo (`etl_pipeline.log`)
- Niveles configurables (INFO, DEBUG)
- Tracking de operaciones SQLAlchemy
- Reporting de errores detallado

## 🌐 Integración con Supabase

### Configuración
1. Crear proyecto en Supabase
2. Configurar variables de entorno
3. Ejecutar: `python main_v2.py supabase`

### Ventajas
- Base de datos en la nube
- API REST automática
- Dashboard web incluido
- Escalabilidad automática
- Backup y seguridad gestionados

## 🧪 Testing

```bash
# Ejecutar pruebas existentes
python Test/test_qualifying_transformer.py

# Verificar conexión BD
python utils.py check-db

# Probar pipeline completo
python main_v2.py --debug
```

## 📈 Rendimiento

### Optimizaciones Implementadas
- **Índices BD**: Optimización de consultas frecuentes
- **Batch processing**: Carga en lotes configurables
- **Connection pooling**: Reutilización de conexiones
- **Lazy loading**: Carga bajo demanda de relaciones
- **Query optimization**: Uso eficiente de JOINs

### Métricas Típicas
- Procesamiento: ~1000 registros/segundo
- Memoria: <100MB para datasets típicos
- Tiempo carga BD: ~2-5 segundos para 10K registros

## 🚨 Troubleshooting

### Problemas Comunes

#### Error de conexión a BD
```bash
# Verificar configuración
python utils.py config

# Probar conexión
python utils.py check-db

# Recrear tablas
python utils.py setup-db
```

#### Errores de migración
```bash
# Ver estado actual
alembic current

# Aplicar migraciones pendientes
python utils.py upgrade

# En caso de conflicto, generar nueva migración
python utils.py migrate
```

#### Datos inconsistentes
```bash
# Ver muestra de datos
python main_v2.py show

# Ejecutar validación
python main_v2.py --debug

# Revisar logs
tail -f etl_pipeline.log
```

## 📝 Logs y Monitoreo

- **Archivo de log**: `etl_pipeline.log`
- **Formato**: Timestamp, nivel, módulo, mensaje
- **Rotación**: Manual (configurar logrotate si necesario)
- **Niveles**: INFO (normal), DEBUG (detallado)

## 🛣️ Roadmap

### Próximas Funcionalidades
- [ ] API REST con FastAPI
- [ ] Dashboard web con Streamlit
- [ ] Procesamiento de otros tipos de datos F1
- [ ] Cache con Redis
- [ ] Métricas con Prometheus
- [ ] Containerización con Docker
- [ ] CI/CD con GitHub Actions

## 🤝 Contribución

1. Fork del repositorio
2. Crear branch para feature
3. Implementar cambios
4. Agregar tests si aplica
5. Crear Pull Request

## 📄 Licencia

MIT License - Ver archivo LICENSE para detalles

## 👨‍💻 Autor

**Miguel Bonilla**  
Fecha: Septiembre 2025

---

Para más información o reportar problemas, crear un issue en el repositorio.
