"""Feature pipeline implementation adapted and extended from the provided notebook.

Produces a single parquet file with features for all api_name/familia groups.
Includes cyclical encodings, same-period shifts, rolling slopes, quantiles,
coverage/imputation flags, holiday flags (Colombia) and family-level aggregates.
"""
from typing import Dict, Tuple
import os
import glob
import yaml
import pandas as pd
import numpy as np
import holidays


def load_config(path: str) -> Dict:
    with open(path, 'r', encoding='utf-8') as f:
        return yaml.safe_load(f)


def _rolling_slope(series: pd.Series, window: int) -> pd.Series:
    # compute slope using linear fit on each rolling window
    def slope(x: np.ndarray) -> float:
        if np.isnan(x).all() or len(x) < 2:
            return 0.0
        # x is a 1-d array
        try:
            idx = np.arange(len(x))
            m = np.polyfit(idx, x, 1)[0]
            return float(m)
        except Exception:
            return 0.0

    return series.rolling(window=window, min_periods=2).apply(slope, raw=True)


def build_features(series: pd.Series, cfg: Dict) -> pd.DataFrame:
    """Build feature DataFrame from a single time series indexed by timestamp.

    series: pd.Series with numeric values (llamados) indexed by datetime
    cfg: configuration dict (expects freq, lag_list, rolling_windows, ema_spans, prev_day_shift)
    """
    lag_list = cfg.get('lag_list', [1, 2, 3, 6, 12])
    rolling_windows = cfg.get('rolling_windows', [12, 36, 288])
    ema_spans = cfg.get('ema_spans', [])
    prev_day_shift = cfg.get('prev_day_shift', None)
    freq = cfg.get('freq', '5min')

    full = pd.DataFrame({'llamados': series}).sort_index()
    df = pd.DataFrame(index=full.index)

    # Lags
    for lag in lag_list:
        df[f'lag_{lag}'] = full['llamados'].shift(lag)

    # Differences and pct changes
    for lag in lag_list:
        df[f'diff_lag_{lag}'] = df[f'lag_{lag}'] - df[f'lag_{lag}'].shift(1)
        df[f'pct_chg_lag_{lag}'] = df[f'lag_{lag}'].pct_change().fillna(0)

    # Rolling aggregations (window size is expressed in number of periods)
    for w in rolling_windows:
        r = full['llamados'].rolling(window=w, min_periods=1)
        df[f'roll_sum_{w}'] = r.sum().shift(0)
        df[f'roll_mean_{w}'] = r.mean().shift(0)
        df[f'roll_median_{w}'] = r.median().shift(0)
        df[f'roll_min_{w}'] = r.min().shift(0)
        df[f'roll_max_{w}'] = r.max().shift(0)
        df[f'roll_std_{w}'] = r.std().shift(0).fillna(0)
        df[f'roll_q25_{w}'] = r.quantile(0.25).shift(0).fillna(0)
        df[f'roll_q75_{w}'] = r.quantile(0.75).shift(0).fillna(0)
        # slope
        df[f'roll_slope_{w}'] = _rolling_slope(full['llamados'], w).shift(0).fillna(0)

    # EMA features
    for span in ema_spans:
        df[f'ema_{span}'] = full['llamados'].ewm(span=span, adjust=False).mean().shift(0)

    # Same-period shifts (prev day / prev week) computed from freq
    try:
        periods_per_day = int(pd.Timedelta('1D') / pd.Timedelta(freq))
    except Exception:
        periods_per_day = cfg.get('prev_day_shift', None) or 288

    if prev_day_shift is None:
        prev_day_shift = periods_per_day

    df['prev_day'] = full['llamados'].shift(prev_day_shift)
    df['prev_week'] = full['llamados'].shift(prev_day_shift * 7)

    # Calendar features
    df['hour'] = full.index.hour
    df['dow'] = full.index.dayofweek
    df['hour_sin'] = np.sin(2 * np.pi * df['hour'] / 24)
    df['hour_cos'] = np.cos(2 * np.pi * df['hour'] / 24)
    df['dow_sin'] = np.sin(2 * np.pi * df['dow'] / 7)
    df['dow_cos'] = np.cos(2 * np.pi * df['dow'] / 7)
    df['is_weekend'] = (df['dow'] >= 5).astype(int)
    df['month'] = full.index.month
    df['day_of_month'] = full.index.day
    df['day_of_year'] = full.index.dayofyear

    # Jornada: 0 = mañana (desde 00:00 hasta 12:00 exacto), 1 = tarde (justo después de 12:00 hasta antes de 00:00)
    # Se usa la precisión de minutos para que 12:00 pertenezca a la mañana y 12:01 a la tarde.
    df['minute'] = full.index.minute
    morning_cond = (df['hour'] < 12) | ((df['hour'] == 12) & (df['minute'] == 0))
    # jornada = 0 para mañana, 1 para tarde
    df['jornada'] = (~morning_cond).astype(int)
    # no necesitamos la columna auxiliar de minutos en el output
    df = df.drop(columns=['minute'])

    # Quincenas: early entre días 14 y 16 (incluyendo ambos), late entre días 29..31 y día 1
    df['quincena_early'] = ((df['day_of_month'] >= 14) & (df['day_of_month'] <= 16)).astype(int)
    df['quincena_late'] = ((df['day_of_month'] >= 29) | (df['day_of_month'] == 1)).astype(int)

    # Holiday flag (Colombia)
    years = list(range(full.index.year.min(), full.index.year.max() + 1))
    try:
        co_holidays = holidays.CountryHoliday('CO', years=years)
        df['holiday'] = [1 if d in co_holidays else 0 for d in full.index.date]
    except Exception:
        # if holidays package not available or fails, default 0
        df['holiday'] = 0

    df['llamados'] = full['llamados']

    # Additional aggregated features requested:
    # 1) prev_dia_com_* -> metrics computed over the full previous day (00:00-23:59:59)
    # 2) prev_dow_com_* -> metrics computed over the previous calendar week (Mon-Sun)
    # 3) prev_dow_interval_* -> metrics for the same time-of-day across all days of the previous week
    # 4) prev_dow_day_* -> metrics over the full day (00:00-23:59:59) for the same weekday over the last 4 weeks

    # Helper: metrics list and names
    _metrics = {
        'sum': lambda s: s.sum(),
        'mean': lambda s: s.mean(),
        'median': lambda s: s.median(),
        'max': lambda s: s.max(),
        'min': lambda s: s.min(),
        'std': lambda s: s.std(),
        'q25': lambda s: s.quantile(0.25),
        'q75': lambda s: s.quantile(0.75)
    }

    # 1) previous day (full day) aggregations
    try:
        idx_norm = full.index.normalize()
        daily_grp = full['llamados'].groupby(idx_norm)
        daily_agg = daily_grp.agg(['sum', 'mean', 'median', 'max', 'min', 'std'])
        daily_q25 = daily_grp.quantile(0.25)
        daily_q75 = daily_grp.quantile(0.75)
        daily_agg['q25'] = daily_q25
        daily_agg['q75'] = daily_q75

        prev_day_key = idx_norm - pd.Timedelta(days=1)
        # reindex to align with timestamps
        prev_day_stats = daily_agg.reindex(prev_day_key)
        # assign columns with prefix prev_dia_com_
        df['prev_dia_com_sum'] = prev_day_stats['sum'].values
        df['prev_dia_com_mean'] = prev_day_stats['mean'].values
        df['prev_dia_com_median'] = prev_day_stats['median'].values
        df['prev_dia_com_max'] = prev_day_stats['max'].values
        df['prev_dia_com_min'] = prev_day_stats['min'].values
        df['prev_dia_com_std'] = prev_day_stats['std'].values
        df['prev_dia_com_q25'] = prev_day_stats['q25'].values
        df['prev_dia_com_q75'] = prev_day_stats['q75'].values
    except Exception:
        # on any failure, create NaN columns (will be filled later)
        for m in ['sum','mean','median','max','min','std','q25','q75']:
            df[f'prev_dia_com_{m}'] = np.nan

    # 2) previous week (Mon-Sun) full-week aggregations
    try:
        # compute label for previous week's Monday for each timestamp
        this_week_monday = full.index.normalize() - pd.to_timedelta(full.index.weekday, unit='D')
        prev_week_monday = this_week_monday - pd.Timedelta(days=7)
        weekly_grp = full['llamados'].groupby(prev_week_monday)
        weekly_agg = weekly_grp.agg(['sum','mean','median','max','min','std'])
        weekly_q25 = weekly_grp.quantile(0.25)
        weekly_q75 = weekly_grp.quantile(0.75)
        weekly_agg['q25'] = weekly_q25
        weekly_agg['q75'] = weekly_q75

        prev_week_stats = weekly_agg.reindex(prev_week_monday)
        df['prev_dow_com_sum'] = prev_week_stats['sum'].values
        df['prev_dow_com_mean'] = prev_week_stats['mean'].values
        df['prev_dow_com_median'] = prev_week_stats['median'].values
        df['prev_dow_com_max'] = prev_week_stats['max'].values
        df['prev_dow_com_min'] = prev_week_stats['min'].values
        df['prev_dow_com_std'] = prev_week_stats['std'].values
        df['prev_dow_com_q25'] = prev_week_stats['q25'].values
        df['prev_dow_com_q75'] = prev_week_stats['q75'].values
    except Exception:
        for m in ['sum','mean','median','max','min','std','q25','q75']:
            df[f'prev_dow_com_{m}'] = np.nan

    # 3) previous week - same interval across days (e.g., same time-of-day in Mon..Sun of prev week)
    try:
        tod = full.index.time
        # group by (prev_week_monday, time_of_day)
        tuple_index = list(zip(prev_week_monday, tod))
        grp = full['llamados'].groupby(tuple_index)
        # build DataFrame of aggregates keyed by (week_monday, time)
        agg_dict = {
            'sum': grp.sum(),
            'mean': grp.mean(),
            'median': grp.median(),
            'max': grp.max(),
            'min': grp.min(),
            'std': grp.std(),
            'q25': grp.quantile(0.25),
            'q75': grp.quantile(0.75)
        }
        # agg_dict entries are Series indexed by tuple (week_monday, time)
        # create a MultiIndex to reindex
        multi_idx = pd.MultiIndex.from_tuples(tuple_index)
        # for assignment, reindex each metric series to the multi_idx
        for k, s in agg_dict.items():
            # align to each timestamp's tuple key
            vals = s.reindex(multi_idx).values
            df[f'prev_dow_interval_{k}'] = vals
    except Exception:
        for m in ['sum','mean','median','max','min','std','q25','q75']:
            df[f'prev_dow_interval_{m}'] = np.nan

    # 4) previous same weekday full-day across last 4 weeks (concatenate full days of that weekday)
    try:
        # build mapping date -> array of values for that date
        date_groups = full['llamados'].groupby(full.index.normalize())
        date_values = {d: g.values for d, g in date_groups}
        # helper to compute metrics on concatenated arrays
        def _arr_metrics(arrs):
            if not arrs:
                return {k: np.nan for k in _metrics}
            a = np.concatenate(arrs)
            return {
                'sum': float(a.sum()),
                'mean': float(a.mean()),
                'median': float(np.median(a)),
                'max': float(a.max()),
                'min': float(a.min()),
                'std': float(a.std(ddof=0)),
                'q25': float(np.percentile(a, 25)),
                'q75': float(np.percentile(a, 75))
            }

        # for each timestamp compute previous 4 same-weekday dates
        prev4_sum = []
        prev4_mean = []
        prev4_median = []
        prev4_max = []
        prev4_min = []
        prev4_std = []
        prev4_q25 = []
        prev4_q75 = []

        for ts in full.index:
            vals = []
            for k in range(1, 5):
                d = (ts.normalize() - pd.Timedelta(days=7 * k))
                arr = date_values.get(d)
                if arr is not None and len(arr) > 0:
                    vals.append(arr)
            metrics = _arr_metrics(vals)
            prev4_sum.append(metrics['sum'])
            prev4_mean.append(metrics['mean'])
            prev4_median.append(metrics['median'])
            prev4_max.append(metrics['max'])
            prev4_min.append(metrics['min'])
            prev4_std.append(metrics['std'])
            prev4_q25.append(metrics['q25'])
            prev4_q75.append(metrics['q75'])

        df['prev_dow_day_sum'] = prev4_sum
        df['prev_dow_day_mean'] = prev4_mean
        df['prev_dow_day_median'] = prev4_median
        df['prev_dow_day_max'] = prev4_max
        df['prev_dow_day_min'] = prev4_min
        df['prev_dow_day_std'] = prev4_std
        df['prev_dow_day_q25'] = prev4_q25
        df['prev_dow_day_q75'] = prev4_q75
    except Exception:
        for m in ['sum','mean','median','max','min','std','q25','q75']:
            df[f'prev_dow_day_{m}'] = np.nan

    # forward fill then fill remaining na with 0
    return df.ffill().fillna(0)


def _prepare_series_from_df(df_hist: pd.DataFrame, freq: str, api: str, fam: str) -> Tuple[pd.Series, pd.DatetimeIndex]:
    """Return a resampled/filled series and an index of timestamps that originally had data.
    The second return value is used to compute imputation flags later.
    """
    mask = (df_hist['api_name'] == api) & (df_hist['familia'] == fam)
    sel = df_hist.loc[mask].copy()

    # Ensure a datetime index exists (construct from columns if needed)
    if {'anio', 'mes', 'dia', 'hora'}.issubset(sel.columns):
        sel = sel.assign(fecha_hora=pd.to_datetime(
            sel[['anio', 'mes', 'dia']].astype(str).agg('-'.join, axis=1) + ' ' + sel['hora']
        )).set_index('fecha_hora')
    else:
        sel.index = pd.to_datetime(sel.index)

    orig_series = sel['llamados']
    # resample sum (period aggregation). Periods without any rows will be NaN.
    s_resampled = orig_series.resample(freq).sum()
    present_mask = ~s_resampled.isna()

    # build full index spanning min/max
    full_index = pd.date_range(s_resampled.index.min(), s_resampled.index.max(), freq=freq)
    s = s_resampled.reindex(full_index)
    # forward fill and fill remaining with 0
    s_filled = s.ffill().fillna(0)

    # align present_mask to full index
    present_mask = present_mask.reindex(full_index, fill_value=False)
    present_index = full_index[present_mask.values]
    return s_filled, present_index


def build_and_save_features(config_path: str):
    cfg = load_config(config_path)
    historic_path = cfg.get('historic_path')
    out_dir = cfg.get('output_dir', 'src/vsti_vapi_modelo_predictivo_apis_dev/static/feature_pipeline_output')
    features_cfg = cfg.get('features', {})
    freq = features_cfg.get('freq', '5min')

    os.makedirs(out_dir, exist_ok=True)

    # Load historic data
    def _read_historic(path: str) -> pd.DataFrame:
        # if path is a directory, read all CSV/XLS/XLSX files inside
        if os.path.isdir(path):
            files = sorted(glob.glob(os.path.join(path, '*')))
            dfs = []
            for f in files:
                if f.lower().endswith('.csv'):
                    dfs.append(pd.read_csv(f))
                elif f.lower().endswith(('.xls', '.xlsx')):
                    dfs.append(pd.read_excel(f))
            if not dfs:
                raise FileNotFoundError(f'No CSV/XLS files found in directory: {path}')
            return pd.concat(dfs, ignore_index=True)

        # single file
        if path.lower().endswith('.csv'):
            return pd.read_csv(path)
        if path.lower().endswith(('.xls', '.xlsx')):
            return pd.read_excel(path)
        # unsupported
        raise ValueError(f'Unsupported historic_path: {path}')

    df_hist = _read_historic(historic_path).drop_duplicates()
    # create fecha_hora index used in the notebook
    if {'anio', 'mes', 'dia', 'hora'}.issubset(df_hist.columns):
        df_hist = df_hist.assign(fecha_hora=lambda df: pd.to_datetime(
            df[['anio', 'mes', 'dia']].astype(str).agg('-'.join, axis=1) + ' ' + df['hora']
        )).set_index('fecha_hora')
    else:
        # if already has datetime index
        df_hist.index = pd.to_datetime(df_hist.index)

    # Precompute family-level aggregated series
    families = df_hist['familia'].dropna().unique().tolist()
    family_series = {}
    for fam in families:
        sel = df_hist[df_hist['familia'] == fam].copy()
        # ensure datetime index
        if {'anio', 'mes', 'dia', 'hora'}.issubset(sel.columns):
            sel = sel.assign(fecha_hora=pd.to_datetime(
                sel[['anio', 'mes', 'dia']].astype(str).agg('-'.join, axis=1) + ' ' + sel['hora']
            )).set_index('fecha_hora')
        else:
            sel.index = pd.to_datetime(sel.index)
        fam_series = sel['llamados'].resample(freq).sum()
        family_series[fam] = fam_series

    apis = df_hist[['api_name', 'familia']].drop_duplicates().values.tolist()

    pieces = []
    for api, fam in apis:
        s, present_index = _prepare_series_from_df(df_hist, freq, api, fam)
        feats = build_features(s, {
            'lag_list': features_cfg.get('lag_list', [1, 2, 3, 6, 12]),
            'rolling_windows': features_cfg.get('rolling_windows', [12, 36, 288]),
            'ema_spans': features_cfg.get('ema_spans', []),
            'prev_day_shift': features_cfg.get('prev_day_shift', None),
            'freq': freq
        })

        # add identifiers
        feats = feats.reset_index().rename(columns={'index': 'fecha_hora'})
        feats['api_name'] = api
        feats['familia'] = fam

        # imputed flag (1 if value was filled/imputed, 0 if original data present)
        feats['imputed_flag'] = (~feats['fecha_hora'].isin(present_index)).astype(int)

        # add family-level rolling means for configured windows
        fam_series = family_series.get(fam)
        if fam_series is not None:
            fam_s = fam_series.reindex(feats['fecha_hora']).ffill().fillna(0)
            for w in features_cfg.get('rolling_windows', [12, 36, 288]):
                feats[f'family_roll_mean_{w}'] = fam_s.rolling(window=w, min_periods=1).mean().values

        pieces.append(feats)

    if pieces:
        out_df = pd.concat(pieces, ignore_index=True, sort=False)
    else:
        out_df = pd.DataFrame()

    #intervenir aqui para crear el query y ejecutar ocn el helper
    out_path = os.path.join(out_dir, 'features.parquet')
    # save parquet with pyarrow
    out_df.to_parquet(out_path, engine='pyarrow', index=False)
    print(f"Saved features parquet to: {out_path}")

    # also save a small manifest with shape and sample
    manifest = {
        'rows': int(out_df.shape[0]),
        'cols': int(out_df.shape[1]) if out_df.shape[1] else 0,
        'path': out_path
    }
    with open(os.path.join(out_dir, 'manifest.yaml'), 'w', encoding='utf-8') as f:
        yaml.safe_dump(manifest, f)

    return out_path


if __name__ == '__main__':
    # simple local test when executed directly: try to find config in same folder
    cfg_path = os.path.join(os.path.dirname(__file__), 'config.yaml')
    if not os.path.exists(cfg_path):
        print('Please run via run_feature_pipeline.py or provide a config path')
    else:
        build_and_save_features(cfg_path)
