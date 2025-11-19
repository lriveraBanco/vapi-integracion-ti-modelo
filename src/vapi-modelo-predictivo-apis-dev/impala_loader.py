"""Impala helper wrapper.

This module tries to use the `impala_helper` (impala-helper) package if available
and falls back to `impyla` (impala.dbapi) if not. The exact high-level helper
functions used from impala-helper will be detected dynamically. If you prefer a
strict implementation against the impala-helper API, paste the example usage
from the PDF and I will wire it exactly.
"""
from typing import Dict
import pandas as pd
import importlib
import logging

#Cambio necesario ------------------------------------------------------------------------------------------------------------
#Modificar lo necesario para que ejecute en el Orquestador de Impala-helper 

_LOGGER = logging.getLogger(__name__)

# basic dtype mapping for DataFrame -> Impala/Hive types
_TYPE_MAP = {
    'int64': 'BIGINT',
    'float64': 'DOUBLE',
    'object': 'STRING',
    'bool': 'BOOLEAN',
    'datetime64[ns]': 'TIMESTAMP'
}


def _map_dtype(dtype) -> str:
    s = str(dtype)
    return _TYPE_MAP.get(s, 'STRING')


def _get_impala_executor():
    """Return a callable exec(sql, **conn_kwargs) that executes SQL and returns rows or None.

    Tries to load impala-helper first (module name 'impala_helper' or 'impala-helper'),
    and falls back to impala.dbapi.connect from impyla if available.
    """
    # Try impala-helper (module name may be 'impala_helper')
    for mod_name in ('impala_helper', 'impala-helper', 'ImpalaHelper'):
        try:
            mod = importlib.import_module(mod_name)
            _LOGGER.debug('Using impala-helper module: %s', mod_name)

            # Common patterns: impala-helper might expose a simple `execute` or a `Client`/`Connector` class.
            if hasattr(mod, 'execute') and callable(mod.execute):
                def _exec(sql, **conn_kwargs):
                    return mod.execute(sql, **conn_kwargs)

                return _exec

            # try to find a connector factory
            if hasattr(mod, 'connect') and callable(mod.connect):
                def _exec(sql, **conn_kwargs):
                    conn = mod.connect(**conn_kwargs)
                    try:
                        res = conn.execute(sql)
                        return res
                    finally:
                        try:
                            conn.close()
                        except Exception:
                            pass

                return _exec

            # try client classes
            for attr in dir(mod):
                if attr.lower().startswith('client') or attr.lower().startswith('connector'):
                    C = getattr(mod, attr)
                    try:
                        def _exec(sql, **conn_kwargs):
                            client = C(**conn_kwargs)
                            try:
                                if hasattr(client, 'execute'):
                                    return client.execute(sql)
                                if hasattr(client, 'run'):
                                    return client.run(sql)
                            finally:
                                try:
                                    client.close()
                                except Exception:
                                    pass

                        return _exec
                    except Exception:
                        continue

            _LOGGER.warning('impala-helper found but no known execute/connect API detected; please provide usage sample from docs')
        except Exception:
            continue

    # Fallback to impyla (impyla must be installed)
    try:
        impala_dbapi = importlib.import_module('impala.dbapi')

        def _exec(sql, **conn_kwargs):
            conn = impala_dbapi.connect(host=conn_kwargs.get('host'),
                                        port=conn_kwargs.get('port', 21050),
                                        user=conn_kwargs.get('user'),
                                        password=conn_kwargs.get('password'),
                                        auth_mechanism=conn_kwargs.get('auth_mechanism', 'PLAIN'))
            cur = conn.cursor()
            cur.execute(sql)
            try:
                res = cur.fetchall()
            except Exception:
                res = None
            cur.close()
            conn.close()
            return res

        _LOGGER.debug('Using impyla fallback')
        return _exec
    except Exception:
        raise ImportError('Neither impala-helper nor impyla (impala.dbapi) are available; please install one of them')


def create_external_table_from_parquet(conn_kwargs: Dict, database: str,
                                       table_name: str, hdfs_parquet_path: str,
                                       df_sample: pd.DataFrame = None, overwrite: bool = False):
    """Create an external Impala table pointing to an existing Parquet location.

    conn_kwargs: dict with connection parameters (host, port, user, password, ...)
    df_sample: optional pandas DataFrame to infer column names/types.
    """
    exec_fn = _get_impala_executor()

    if df_sample is not None:
        cols = []
        for c, dtype in zip(df_sample.columns, df_sample.dtypes):
            cols.append(f"`{c}` {_map_dtype(dtype)}")
        cols_sql = ',\n  '.join(cols)
        create_sql = f"CREATE EXTERNAL TABLE {'IF NOT EXISTS' if not overwrite else ''} {database}.{table_name} (\n  {cols_sql}\n) STORED AS PARQUET LOCATION '{hdfs_parquet_path}'"
    else:
        create_sql = f"CREATE EXTERNAL TABLE {'IF NOT EXISTS' if not overwrite else ''} {database}.{table_name} STORED AS PARQUET LOCATION '{hdfs_parquet_path}'"

    return exec_fn(create_sql, **conn_kwargs)


def run_sql(conn_kwargs: Dict, sql: str):
    exec_fn = _get_impala_executor()
    return exec_fn(sql, **conn_kwargs)



#Cambio necesario ------------------------------------------------------------------------------------------------------------
