"""Generate a small sample historic CSV and test Excel to validate the pipeline locally.

This script writes two files (CSV and Excel) compatible with the pipeline config
paths used in the repo. Use for quick local sanity checks.
"""
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import os


def generate_sample(historic_csv_path: str, test_xlsx_path: str, start: str = '2025-03-01', days: int = 3, freq: str = '5min'):
    start_dt = pd.to_datetime(start)
    periods_per_day = int(pd.Timedelta('1D') / pd.Timedelta(freq))
    total_periods = periods_per_day * days
    idx = pd.date_range(start_dt, periods=total_periods, freq=freq)

    apis = [
        ('api_A', 'fam1'),
        ('api_B', 'fam1'),
        ('api_C', 'fam2')
    ]

    rows = []
    for api_name, fam in apis:
        # simple pattern with noise
        base = 50 if fam == 'fam1' else 10
        for ts in idx:
            rows.append({
                'anio': ts.year,
                'mes': ts.month,
                'dia': ts.day,
                'hora': ts.strftime('%H:%M:%S'),
                'api_name': api_name,
                'familia': fam,
                'llamados': max(0, int(base + 10 * (np.sin((ts.hour + ts.minute/60)/24 * 2 * np.pi)) + np.random.randn() * 3))
            })

    df = pd.DataFrame(rows)
    os.makedirs(os.path.dirname(historic_csv_path), exist_ok=True)
    df.to_csv(historic_csv_path, index=False)

    # create a small test excel (one day) for prediction
    df_test = df[df['dia'] == start_dt.day].copy()
    os.makedirs(os.path.dirname(test_xlsx_path), exist_ok=True)
    df_test.to_excel(test_xlsx_path, index=False)

    print(f"Wrote sample historic CSV -> {historic_csv_path}")
    print(f"Wrote sample test XLSX -> {test_xlsx_path}")


if __name__ == '__main__':
    base_dir = os.path.join(os.path.dirname(__file__), 'static', 'excel')
    os.makedirs(base_dir, exist_ok=True)
    hist = os.path.join(base_dir, 'Metricas_sample.csv')
    test = os.path.join(base_dir, 'mes_marzo_sample.xlsx')
    generate_sample(hist, test)
