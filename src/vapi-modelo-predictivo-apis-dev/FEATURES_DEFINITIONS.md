# Definición técnica de las variables (features)

Este archivo documenta, para cada columna generada por el pipeline, su nombre,
cómo se calcula (fórmula/algoritmo) y una breve definición técnica/propósito.

Nota: algunas variables dependen de parámetros del `config.yaml` como `freq`,
`lag_list`, `rolling_windows` o `ema_spans`. Cuando se indica `w` o `span` se
refiere a valores de esas listas.

---

## Identificadores y metadatos

- `fecha_hora` (datetime)
  - Cálculo: índice datetime del DataFrame final (resampleado según `freq`).
  - Definición: marca temporal de la fila (periodo de agregación).

- `api_name` (string)
  - Cálculo: proviene de la columna original `api_name` en los datos históricos.
  - Definición: identificador del API al que corresponde la serie.

- `familia` (string)
  - Cálculo: proviene de la columna original `familia` en los datos históricos.
  - Definición: grupo o familia funcional del API.

- `imputed_flag` (int: 0/1)
  - Cálculo: 1 si el timestamp fue rellenado/imputado (no estaba presente en el
    conjunto histórico original), 0 si el periodo tenía datos originales.
  - Definición: indicador de filas imputadas tras el resample/align.

---

## Serie base

- `llamados` (numeric, int/float)
  - Cálculo: valor agregado (suma) por periodo tras resampleo de la serie
    original (`orig_series.resample(freq).sum()`), luego forward-fill y `fillna(0)`.
  - Definición: conteo/volumen de llamadas en el periodo.

---

## Lags, diferencias y cambios porcentuales

Para cada `lag` en `lag_list` (por ejemplo 1,2,3,6,12):

- `lag_{lag}` (numeric)
  - Cálculo: `llamados.shift(lag)`.
  - Definición: valor de `llamados` en el periodo desplazado `lag` pasos atrás.

- `diff_lag_{lag}` (numeric)
  - Cálculo: `lag_{lag} - lag_{lag}.shift(1)` (diferencia entre el valor de ese
    lag y su periodo previo).
  - Definición: cambio absoluto en la serie del mismo lag respecto al periodo
    anterior (captura aceleraciones a ese desfase).

- `pct_chg_lag_{lag}` (numeric)
  - Cálculo: `lag_{lag}.pct_change().fillna(0)`.
  - Definición: cambio porcentual del valor en el lag respecto al periodo
    anterior (normaliza por magnitud).

---

## Rolling aggregations (ventanas) — para cada `w` en `rolling_windows`

Estas ventanas están expresadas en número de periodos según `freq`.

- `roll_sum_{w}`
  - Cálculo: `full['llamados'].rolling(window=w, min_periods=1).sum()`
  - Definición: suma de `llamados` en la ventana móvil de tamaño `w` que termina
    en el periodo actual.

- `roll_mean_{w}`
  - Cálculo: `.rolling(window=w, min_periods=1).mean()`
  - Definición: media móvil aritmética de `llamados` en la ventana `w`.

- `roll_median_{w}`
  - Cálculo: `.rolling(...).median()`
  - Definición: mediana en la ventana `w` (robusta a outliers).

- `roll_min_{w}` / `roll_max_{w}`
  - Cálculo: `.rolling(...).min()` / `.max()`
  - Definición: mínimo/máximo observados en la ventana.

- `roll_std_{w}`
  - Cálculo: `.rolling(...).std().fillna(0)`
  - Definición: desviación estándar en la ventana (`0` si NaN).

- `roll_q25_{w}` / `roll_q75_{w}`
  - Cálculo: `.rolling(...).quantile(0.25)` / `quantile(0.75)`
  - Definición: percentiles 25 y 75 en la ventana (distribución local).

- `roll_slope_{w}`
  - Cálculo: pendiente de ajuste lineal por mínimos cuadrados sobre los
    valores de `llamados` en la ventana (función `_rolling_slope`) — retorna 0
    en caso de error o pocos datos.
  - Definición: tasa de cambio lineal local (pendiente) en la ventana `w`.

---

## EMA (medias móviles exponenciales)

Para cada `span` en `ema_spans`:

- `ema_{span}`
  - Cálculo: `full['llamados'].ewm(span=span, adjust=False).mean()`
  - Definición: promedio exponencial con factor de smoothing determinado por
    `span` (más reciente tiene mayor peso).

---

## Same-period shifts

- `prev_day` (numeric)
  - Cálculo: `full['llamados'].shift(prev_day_shift)` donde `prev_day_shift`
    viene de `features.prev_day_shift` o se calcula como `periods_per_day` según
    `freq` (ej. `5min` -> 288).
  - Definición: valor de `llamados` exactamente 1 día atrás (mismo horario).

- `prev_week` (numeric)
  - Cálculo: `full['llamados'].shift(prev_day_shift * 7)`
  - Definición: valor de `llamados` 1 semana atrás.

---

## Características de calendario

- `hour` (int)
  - Cálculo: `full.index.hour`.
  - Definición: hora del día (0-23).

- `dow` (int)
  - Cálculo: `full.index.dayofweek` (0 = lunes, 6 = domingo en pandas).
  - Definición: día de la semana.

- `hour_sin`, `hour_cos` (float)
  - Cálculo: codificación cíclica: `sin(2*pi*hour/24)` y `cos(2*pi*hour/24)`.
  - Definición: representación continua y cíclica de la hora (evita discontinuidad 23->0).

- `dow_sin`, `dow_cos` (float)
  - Cálculo: `sin(2*pi*dow/7)` y `cos(2*pi*dow/7)`.
  - Definición: codificación cíclica del día de la semana.

- `is_weekend` (int: 0/1)
  - Cálculo: `(dow >= 5).astype(int)`.
  - Definición: indicador si el día es sábado o domingo.

- `month` (int)
  - Cálculo: `full.index.month`.
  - Definición: mes del año (1-12).

- `day_of_month` (int)
  - Cálculo: `full.index.day`.
  - Definición: día numérico del mes (1-31).

- `day_of_year` (int)
  - Cálculo: `full.index.dayofyear`.
  - Definición: día del año (1-366).

---

## Flag de festivo (holiday)

- `holiday` (int: 0/1)
  - Cálculo: usando la librería `holidays` con país `'CO'` (Colombia):
    `1 if fecha in co_holidays else 0`.
  - Definición: indicador de día festivo en Colombia. Si la librería falla,
    se asigna 0 para todas las filas.

---

## Campos agregados a nivel de familia

Para cada `w` en `rolling_windows`:

- `family_roll_mean_{w}` (numeric)
  - Cálculo: para la familia correspondiente, se calcula la serie agregada
    `fam_series = fam_series.reindex(fecha_hora).ffill().fillna(0)` y luego
    `fam_s.rolling(window=w, min_periods=1).mean()` (aligned con cada fila).
  - Definición: media móvil de la suma de `llamados` de la familia en la
    ventana `w`, usada como feature agregada a nivel familia.

---

## Variables añadidas explícitamente (solicitudes recientes)

- `jornada` (int: 0/1)
  - Cálculo: se toma `hour` y `minute`; `morning_cond = (hour < 12) or (hour == 12 and minute == 0)`
    y `jornada = (~morning_cond).astype(int)`.
  - Definición técnica: indicador binario de turno del día donde
    - 0 = mañana (desde 00:00 hasta 12:00 exacto, inclusive 12:00:00),
    - 1 = tarde (desde justo después de 12:00:00 hasta antes de 00:00).
  - Nota: se usó la precisión de minutos para asignar 12:00 exactamente a la mañana.

- `quincena_early` (int: 0/1)
  - Cálculo: `((day_of_month >= 14) & (day_of_month <= 16)).astype(int)`.
  - Definición técnica: indicador del periodo "quincena early" definido
    operacionalmente como días 14, 15 y 16 del mes.

- `quincena_late` (int: 0/1)
  - Cálculo: `((day_of_month >= 29) | (day_of_month == 1)).astype(int)`.
  - Definición técnica: indicador del periodo "quincena late" definido
    operacionalmente como días 29..31 y día 1 del mes (incluye inicio de mes).

---

## Agregados históricos solicitados (prev_*)

Las siguientes familias de variables se añadieron para capturar información
histórica en distintos niveles de granularidad. Para cada prefijo se generan
las métricas: `sum`, `mean`, `median`, `max`, `min`, `std`, `q25`, `q75`.

- Prefijo `prev_dia_com_` (ej. `prev_dia_com_sum`)
  - Cálculo: para cada timestamp se toma el día calendario completo anterior
    (desde 00:00:00 hasta 23:59:59 del día t-1) sobre la misma `api_name`/`familia`
    y se calculan las métricas agregadas listadas arriba.
  - Definición técnica: resumen estadístico del día previo completo; útil
    para captar cambios diarios y patrones de volumen diarios.

- Prefijo `prev_dow_com_` (ej. `prev_dow_com_mean`)
  - Cálculo: para cada timestamp se identifica la semana calendario previa
    (lunes–domingo inmediatamente anterior) y se computan las métricas sobre
    todos los intervalos de esa semana para la misma serie.
  - Definición técnica: resumen estadístico de la semana previa completa,
    captura tendencias semanales y anomalías en la semana anterior.

- Prefijo `prev_dow_interval_` (ej. `prev_dow_interval_q75`)
  - Cálculo: para cada timestamp con cierto time-of-day (p. ej. 19:00), se
    toman los valores de ese mismo time-of-day en cada día de la semana previa
    (7 valores: lunes..domingo de la semana previa) y se calculan las métricas.
  - Definición técnica: caracteriza el comportamiento del mismo intervalo
    horario en la semana previa (útil para patrones intradía repetitivos).

- Prefijo `prev_dow_day_` (ej. `prev_dow_day_median`)
  - Cálculo: para el weekday correspondiente al timestamp (por ejemplo,
    miércoles), se seleccionan las 4 ocurrencias previas de ese weekday
    (las mismas fechas en las 4 semanas anteriores), se concatenan los
    valores de cada día completo (00:00–23:59:59) y sobre el conjunto resultante
    se calculan las métricas.
  - Definición técnica: resume el comportamiento diario repetido del mismo
    weekday en el histórico reciente (últimas 4 semanas), útil para captar
    patrones semanales estables o drifts.

Notas sobre implementación:
- Todas estas agregaciones se calculan únicamente utilizando la serie de la
  misma `api_name`/`familia` para evitar fuga entre series.
- Los nombres concretos generados en `pipeline.py` siguen el patrón
  `{prefix}_{metric}` (por ejemplo `prev_dia_com_std`, `prev_dow_interval_q25`).
- En presencia de ausencia de datos, las columnas son inicialmente NaN pero
  al final del `build_features` se aplica `ffill().fillna(0)` (esto es coherente
  con el resto de features). Si prefieres mantener NaNs, puedo cambiar ese
  comportamiento.


## Notas generales y edge cases

- Tipos: la mayoría de features numéricas provienen de `llamados` y serán
  `float64` cuando pandas realice operaciones (rolling, mean, slope). Algunos
  indicadores son `int` (0/1). Asegúrese de castearlos si requiere tipos
  específicos antes de entrenar modelos.
- Ventanas y spans: si una ventana tiene pocos datos, `min_periods=1` evita NaNs
  excepto en operaciones específicas (p. ej. slope tiene min_periods=2 internamente).
- Holiday: la dependencia a la librería `holidays` puede no estar instalada en
  todos los entornos; en ese caso `holiday` será 0.
- Imputación: las filas imputadas se marcan con `imputed_flag=1`; esto permite
  filtrar o añadir lógica de confianza en downstream.

Si quieres, puedo:
- generar automáticamente un CSV con esta documentación (por si la quieres
  importar a una herramienta de catalogación), o
- añadir pruebas unitarias que verifiquen la existencia y formato de estas
  columnas tras ejecutar `build_features` con un `sample_series`.

Fin de `FEATURES_DEFINITIONS.md`
