Runner del pipeline de features
================================

Este repositorio incluye un runner sencillo para ejecutar la extracción de
features que utilizan los modelos de entrenamiento.

Archivos relevantes:
- `src\run_feature_pipeline.py`: script que ejecuta el pipeline usando el
    `config.yaml` incluido o un archivo de configuración que usted indique.
- `src\requirements.txt`: dependencias necesarias para ejecutar el pipeline.

Cómo ejecutar (Windows PowerShell):

1. Crear y activar un entorno virtual (recomendado):

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
```

2. Instalar dependencias (todas desde `src/requirements.txt`):

```powershell
pip install -r src\requirements.txt
```

3. Ejecutar el pipeline desde la raíz del repo:

```powershell
    python src\run_feature_pipeline.py [ruta/opcional/a/config.yaml]
```

Observaciones
- El runner importa `pipeline.py` por ruta de archivo para evitar problemas
    con nombres de paquete que contengan guiones. Si reorganiza el código a una
    estructura de paquete convencional, puede importar normalmente.
 
Documentación detallada (en español)
-----------------------------------

Este documento explica en detalle los archivos implicados y los pasos para
ejecutar el pipeline de features localmente en Windows PowerShell.

1) Archivos principales y su propósito
- `src/run_feature_pipeline.py`: runner que carga `pipeline.py` desde su ruta
    y ejecuta `build_and_save_features(config_path)`. Acepta un argumento opcional
    con la ruta al `config.yaml` a usar.
- `src/vapi-modelo-predictivo-apis-dev/pipeline.py`: implementación del
    pipeline. Funciones clave:
    - `build_and_save_features(config_path)`: lee el historic_path del config,
        procesa series por `api_name`/`familia`, construye features y salva
        `feature_pipeline_output/features.parquet`.
    - `build_features(series, cfg)`: genera las columnas de features para una
        serie (incluye calendario, lags, rolling, EMA, holiday, y las nuevas
        `jornada`, `quincena_early`, `quincena_late`).
- `src/vapi-modelo-predictivo-apis-dev/config.yaml`: configuración por defecto
    usada por `run_feature_pipeline.py` si no se pasa otra ruta. Contiene:
    - `historic_path`: ruta al CSV/XLSX o carpeta con datos históricos de "llamados".
    - `test_path`: ruta opcional a archivo de prueba.
    - `output_dir`: carpeta donde se escriben `features.parquet` y `manifest.yaml`.
    - `features`: dict con `freq`, `lag_list`, `rolling_windows`, `ema_spans` y `prev_day_shift`.
- `src/tmp_config.yaml`: ejemplo que apunta a `src/vapi-modelo-predictivo-apis-dev/static/excel`.
- `src/scripts/read_parquet.py`: utilidad CLI para inspeccionar y exportar el
    parquet generado (soporta `--path`, `--head`, `--cols`, `--schema`, `--to-csv`).

2) Pasos detallados para ejecutar (PowerShell)
- Crear y activar entorno virtual (desde la raíz del repo):

```powershell
python -m venv .venv
# si PowerShell bloquea la activación, permitir para la sesión:
Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope Process -Force
.\.venv\Scripts\Activate.ps1
```

- Actualizar pip y herramientas de empaquetado:

```powershell
python -m pip install --upgrade pip setuptools wheel
```

- Instalar dependencias del proyecto:

```powershell
python -m pip install -r src\requirements.txt
# si usa requirements privados:
python -m pip install -r src\requirements-private.txt
```

- Ejecutar el pipeline (usar config por defecto o pasar uno propio):

```powershell
# usa el config interno (por defecto apunta a archivos en src/.../static/excel)
python src\run_feature_pipeline.py

# ó pasar un archivo de configuración alternativo
python src\run_feature_pipeline.py src\tmp_config.yaml
```

Salida esperada:
- El pipeline guarda: `feature_pipeline_output/features.parquet` (parquet)
    y `feature_pipeline_output/manifest.yaml` con filas/columnas y ruta.

3) Verificar columnas y exportar los primeros 50 registros a Excel

Usa la utilidad incluida `src/scripts/read_parquet.py` (recomendada):

```powershell
# Exporta TODOS los campos de los primeros 50 registros a Excel
python src\scripts\read_parquet.py -p feature_pipeline_output\features.parquet --head 50 --to-csv feature_pipeline_output\features_top50.csv
# luego convertir a Excel (usa pandas)
python -c "import pandas as pd; pd.read_csv('feature_pipeline_output\\features_top50.csv').to_excel('feature_pipeline_output\\features_top50.xlsx', index=False); print('Saved Excel')"
```

Comando alternativo one-liner (lee parquet y salva directo a Excel):

```powershell
python -c "import pandas as pd; df=pd.read_parquet(r'feature_pipeline_output\\features.parquet'); df.head(50).to_excel(r'feature_pipeline_output\\features_top50.xlsx', index=False); print('Saved: feature_pipeline_output\\features_top50.xlsx')"
```

Nota: `to_excel` requiere `openpyxl` como motor para archivos .xlsx; instálalo si falta:

```powershell
python -m pip install openpyxl
```

4) Uso de `read_parquet.py` para inspección rápida

Ejemplos útiles:
- Mostrar columnas + head (por defecto head=10):
    `python src\scripts\read_parquet.py -p feature_pipeline_output\features.parquet`
- Mostrar columnas específicas (más rápido):
    `python src\scripts\read_parquet.py -p feature_pipeline_output\features.parquet --cols fecha_hora,jornada,quincena_early,quincena_late --head 50`
- Mostrar esquema (si `pyarrow` instalado):
    `python src\scripts\read_parquet.py -p feature_pipeline_output\features.parquet --schema`

5) Campos nuevos añadidos por el pipeline
- `jornada`: 0 = mañana (00:00 hasta 12:00 exacto), 1 = tarde (justo después de 12:00 hasta antes de 00:00).
- `quincena_early`: 1 si `day_of_month` está entre 14 y 16 (incluidos), 0 otherwise.
- `quincena_late`: 1 si `day_of_month` es >= 29 ó == 1, 0 otherwise.

6) Configuración y personalización
- `features.freq`: frecuencia de resampleo (ej. `5min`). Afecta `prev_day_shift` calculado por número de periodos por día.
- `features.lag_list`, `rolling_windows`, `ema_spans`, `prev_day_shift`: parámetros para lags, ventanas rolling y EMAs.
- `historic_path`: puede apuntar a un archivo CSV/XLSX único o a un directorio con múltiples archivos; el _reader_ admite `.csv`, `.xls` y `.xlsx`.

7) Problemas comunes y soluciones
- FileNotFoundError al leer `historic_path`: revise que la ruta en `config.yaml` exista. Puede usar rutas relativas (recomendado) o pasar un config alternativo a `run_feature_pipeline.py`.
- Error al activar venv en PowerShell: use `Set-ExecutionPolicy -Scope Process -ExecutionPolicy RemoteSigned -Force` antes de activar.
- Al leer parquet: si pandas lanza error por falta de `pyarrow`, instale `pyarrow`:
    `python -m pip install pyarrow`
- Al exportar a Excel: si `to_excel` falla por falta de `openpyxl`, instale:
    `python -m pip install openpyxl`

8) Verificación rápida (comandos recomendados)
- Ejecutar pipeline y luego listar primeros 5 registros con columnas de interés:

```powershell
python src\run_feature_pipeline.py
python src\scripts\read_parquet.py -p feature_pipeline_output\features.parquet --cols fecha_hora,jornada,quincena_early,quincena_late --head 5
```

9) Contacto/seguimiento
- Si necesita que automatice la creación del venv y la instalación (o añada un `Makefile`/`ps1`), puedo agregar scripts de convenience. También puedo añadir validaciones extra al pipeline para fallar con mensajes más claros cuando falte `historic_path`.

Fin de la documentación extendida.

Documentación técnica de archivos del pipeline
---------------------------------------------

A continuación se describen con más detalle los archivos del pipeline, su
propósito, interfaces y comportamiento interno para desarrolladores que deseen
extender o depurar el código.

`src/vapi-modelo-predictivo-apis-dev/pipeline.py`
- Propósito: generar un conjunto consolidado de features (parquet) a partir de
    datos históricos de llamadas por `api_name`/`familia`.
- Interfaz pública principal:
    - `build_and_save_features(config_path: str) -> str` : lee el `config.yaml`,
        carga los datos históricos, agrega features por grupo y escribe `features.parquet`.
        Retorna la ruta al fichero creado.
    - `build_features(series: pd.Series, cfg: Dict) -> pd.DataFrame` : genera las
        columnas de features para una serie temporal (lags, diferencias, rolling,
        EMA, calendar features, holiday flag, etc.).

- Flujo interno resumido:
    1. `build_and_save_features` lee el `historic_path` del config (archivo o
         carpeta). Si es carpeta, concatena todos los CSV/XLS(X) dentro.
    2. Normaliza columnas de fecha (`anio`, `mes`, `dia`, `hora`) a un índice
         `fecha_hora` tipo datetime si existen esas columnas; si el DataFrame ya
         trae un índice datetime, lo usa tal cual.
    3. Prepara series por cada par (`api_name`, `familia`) usando
         `_prepare_series_from_df`, que resamplea por la `freq` configurada y rellena
         periodos faltantes (ffill, luego 0).
    4. Llama a `build_features` por serie para computar lags, rollings, EMAs,
         prev_day/prev_week, y las columnas de calendario (hora, dow, month, etc.).
         También añade las columnas `jornada`, `quincena_early`, `quincena_late`.
    5. Agrega indicadores y features a nivel de familia (`family_roll_mean_*`).
    6. Concatena todos los `feats` en un DataFrame final y lo escribe como
         `feature_pipeline_output/features.parquet` (pyarrow) y un `manifest.yaml`.

- Notas de implementación y extensibilidad:
    - `build_features` usa `pd.Timedelta` para calcular `periods_per_day` según
        la `freq` (ej. `5min` -> 288). `prev_day_shift` puede ser pasado en config
        o deducido.
    - Rolling windows y EMA spans se parametrizan desde `config.yaml`.
    - Holiday flag usa la librería `holidays` para Colombia (`'CO'`); si falla,
        la columna `holiday` será 0.
    - Imputation: la función `_prepare_series_from_df` devuelve además un
        `present_index` que se usa para marcar `imputed_flag` en las filas que fueron
        rellenadas.

`src/vapi-modelo-predictivo-apis-dev/config.yaml` (esquema y opciones)
- Campos principales:
    - `historic_path`: string. Ruta a un archivo CSV/XLS(X) o a un directorio. Si
        apunta a un directorio, el reader concatenará todos los ficheros soportados
        (`.csv`, `.xls`, `.xlsx`). Se aceptan rutas relativas y absolutas.
    - `test_path`: ruta opcional a un archivo de prueba (ej. para predicción).
    - `output_dir`: carpeta donde se escribirán `features.parquet` y `manifest.yaml`.
    - `features`: diccionario con los parámetros de generación de features:
        - `freq`: frecuencia de resampleo (string, p.ej. `5min`, `1H`).
        - `lag_list`: lista de enteros con lags a generar (p.ej. `[1,2,3,6,12]`).
        - `rolling_windows`: lista de tamaños de ventana (en número de periodos
            según `freq`) para rolling sums/means/std/quantiles/slope.
        - `ema_spans`: lista de spans para medias móviles exponenciales.
        - `prev_day_shift`: opcional; número de periodos que corresponde a un día.

- Recomendaciones de uso:
    - Para reproducibilidad, versiona el `config.yaml` usado en cada ejecución
        (por ejemplo guardándolo dentro de `feature_pipeline_output/` junto al parquet).
    - Si `historic_path` apunta a una fuente externa (S3, HDFS), prepara antes
        un paso que copie los ficheros a la ruta local o modifica `_read_historic`
        para soportar la URL/driver correspondiente.

`src/vapi-modelo-predictivo-apis-dev/impala_loader.py`
- Propósito: helpers para ejecutar SQL en Impala y crear tablas externas a
    partir de un parquet en HDFS/Impala. El módulo intenta cargar `impala-helper`
    (si está disponible) y, si no, usa `impyla` (`impala.dbapi`) como fallback.

- Funciones públicas y comportamiento:
    - `_get_impala_executor() -> callable`: detecta dinámicamente y devuelve una
        función `exec_fn(sql, **conn_kwargs)` que ejecuta SQL contra Impala.
        - Si se encuentra `impala_helper` (u otra implementación similar), intenta
            usar patrones comunes (`execute`, `connect`, o clases `Client`/`Connector`).
        - Si no, intenta importar `impala.dbapi` y crear una conexión con
            `impala.dbapi.connect(...)`.
        - Si ninguna librería está presente lanza ImportError con indicación.

    - `create_external_table_from_parquet(conn_kwargs, database, table_name, hdfs_parquet_path, df_sample=None, overwrite=False)`:
        - Crea una sentencia `CREATE EXTERNAL TABLE` que apunta a `hdfs_parquet_path`.
        - Si se proporciona `df_sample` (pandas DataFrame), infiere tipos usando
            `_map_dtype` y genera el listado de columnas con tipos (ej. `BIGINT`,
            `DOUBLE`, `STRING`, `BOOLEAN`, `TIMESTAMP`). Si no se pasa `df_sample`,
            crea la tabla sin columnas (impala usará el esquema del parquet).
        - `conn_kwargs` deben incluir los parámetros necesarios para la librería
            usada (p.ej. `host`, `port`, `user`, `password`, `auth_mechanism`).

    - `run_sql(conn_kwargs, sql)`: ejecuta una sentencia SQL usando el executor
        detectado y retorna el resultado (filas) o `None` si no aplica.

- Dependencias y recomendaciones:
    - Para uso en producción en clusters Cloudera/Impala, instala `impyla` o
        `impala-helper` según convenga. `impyla` es la opción de fallback más
        sencilla (`pip install impyla`).
    - El mapeo de dtypes es básico; si tu parquet usa tipos complejos (DECIMAL,
        STRUCT), extiende `_TYPE_MAP` según necesidad.
    - Las funciones no gestionan transacciones ni reintentos; para ambientes
        inestables añade lógica de retry y logging más detallado.

`src/vapi-modelo-predictivo-apis-dev/sample_data.py`
- Propósito: script utilitario para generar datos de ejemplo compatibles con
    el pipeline (CSV histórico y Excel de prueba). Útil para pruebas locales y CI.
- Interfaz: `generate_sample(historic_csv_path, test_xlsx_path, start, days, freq)`
    genera un CSV con columnas `anio`, `mes`, `dia`, `hora`, `api_name`, `familia`, `llamados`.

`src/scripts/read_parquet.py` (resumen)
- CLI para inspeccionar el parquet generado. Soporta opciones:
    - `--path/-p`: ruta al parquet (default `feature_pipeline_output/features.parquet`).
    - `--head`: número de filas a mostrar.
    - `--schema`: muestra metadata via `pyarrow` si disponible.
    - `--cols`: columnas separadas por comas a leer (evita cargar todas las columnas).
    - `--to-csv`: exporta la muestra a CSV.

Ejemplos de uso rápido
- Generar datos de ejemplo (crea archivos en `src/.../static/excel`):
    ```powershell
    python -c "from src.vapi_modelo_predictivo_apis_dev import sample_data as s; s.generate_sample('src/vapi-modelo-predictivo-apis-dev/static/excel/Metricas_sample.csv','src/vapi-modelo-predictivo-apis-dev/static/excel/mes_marzo_sample.xlsx')"
    ```

- Crear tabla externa en Impala (ejemplo con conn kwargs):
    ```python
    from vapi_modelo_predictivo_apis_dev import impala_loader as il
    conn = {'host':'impala.host.local','port':21050,'user':'hive_user'}
    il.create_external_table_from_parquet(conn, 'default', 'features_table', '/path/on/hdfs/features.parquet', df_sample=my_df)
    ```

Notas finales
- Si quieres, puedo añadir pruebas unitarias mínimas (pytest) que verifiquen:
    - `build_features` agrega las columnas esperadas para una serie de ejemplo.
    - `read_parquet.py` devuelve las columnas esperadas.
    - `impala_loader._map_dtype` mapea tipos conocidos correctamente.

Fin de la documentación técnica.

