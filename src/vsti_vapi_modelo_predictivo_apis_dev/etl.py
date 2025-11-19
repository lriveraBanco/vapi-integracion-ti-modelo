# -*- coding: utf-8 -*-

"""
-----------------------------------------------------------------------------
-----------------------------------------------------------------------------
-- Equipo VSTI
-----------------------------------------------------------------------------
-- Fecha Creación: 20251029
-- Última Fecha Modificación: 20251029
-- Autores: lrivera, anpolo
-- Últimos Autores: lrivera, anpolo
-- Descripción: Script de ejecución de los ETLs
-----------------------------------------------------------------------------
-----------------------------------------------------------------------------
"""
from orquestador2.step 	    import Step
from datetime	       		import datetime
from dateutil.relativedelta import relativedelta
import json
import pkg_resources
import os
import sys
import importlib.util
import pandas as pd
from typing import Dict, Tuple
import yaml

class ExtractTransformLoad(Step):
    """
    Clase encargada de la ejecución de los ETLs
    necesarios para extraer y procesar la información
    de interés de la rutina.
    """

    @staticmethod
    def obtener_ruta():
        """
        Función encargada de identificar la
        carpeta static relacionada al paquete
        ------
        Return
        ------
        ruta_src : string
        Ruta static en el sistema o entorno de
        los recursos del paquete
        """
        return pkg_resources.resource_filename(__name__, 'static')

    def obtener_params(self):
        """
        Función encargada de obtener los parámetros
        necesarios para la ejecución del paso.
        ------
        Return
        ------
        params : dictionary
        Parámetros necesarios para ejecutar el paso.
        """
        #PARAMETROS GENERALES DEL PASO
        params = self.getGlobalConfiguration()["parametros_lz"]
        now = datetime.today()
        params_default = {
            "kwargs_year"  : now.year,
            "kwargs_month" : now.month,
            "kwargs_day"   : now.day
        }
        params_default.update(self.kwa)
        now = datetime(
            params_default["kwargs_year"]
            , params_default["kwargs_month"]
            , params_default["kwargs_day"]
        )
        params_calc = {
            #FECHAS
            "f_corte_y"  : \
                str((now + relativedelta(months=-1)).year),
            "f_corte_m"  : \
                str((now + relativedelta(months=-1)).month),
            "f_corte_d"  : \
                str((now + relativedelta(months=-1)).day),
            "f_actual_y" : \
                str(now.year),
            "f_actual_m" : \
                str(now.month),
            "f_actual_d" : \
                str(now.day)
        }
        params.update(params_calc)
        params.update(self.kwa)
        params.pop("password", None)
        return params

    def ejecutar(self):
        """
        Función que ejecuta el paso de la clase.
        """
        self.log.info(json.dumps(
            self.obtener_params(), \
            indent = 4, sort_keys = True))
        self.executeTasks()  
        
    def load_config(self,path: str) -> Dict:
        with open(path, 'r', encoding='utf-8') as f:
            return yaml.safe_load(f)      

    def create_table(self):       
            
       params = self.getGlobalConfiguration()["parametros_lz"]
       
       default_config = os.path.join(os.path.dirname(__file__), "config.yaml")       
       config_path = os.path.join(os.getcwd(), params.get('config_file')) if params.get('config_file') is True else default_config
       
       cfg = self.load_config(config_path)       
       out_dir = cfg.get('output_dir', 'src/vsti_vapi_modelo_predictivo_apis_dev/static/feature_pipeline_output')
       
       #leer el parquet
       parquet_path = os.path.join(os.getcwd(),out_dir)
       parquet = os.path.join(parquet_path, 'features.parquet')
       df_parquet = pd.read_parquet(parquet)
       #obtener los indices
       columns_parquet = list(df_parquet.columns)
                  
       #escribir el sql
       new_sql_path = os.path.join(os.path.dirname(__file__),"static/sql/ExtractTransformLoad","001_create_table.sql")
       
       #elimina el archivo si existe
       if os.path.exists(new_sql_path):
            os.remove(new_sql_path)
       
       query_file = open(new_sql_path,"w")
             
       query_file.write(f"DROP TABLE IF EXISTS {params.get('zona_procesamiento')}.{params.get('prefijo')}{params.get('nombre_tabla')} PURGE;\n")
       query_file.write(f"CREATE TABLE IF NOT EXISTS {params.get('zona_procesamiento')}.{params.get('prefijo')}{params.get('nombre_tabla')} (\n")
       
       fields = [f"{column} STRING" for column in columns_parquet]
       scheme_sql = ",\n".join(fields)
       query_file.write(scheme_sql)    
       
       query_file.write("\n)\nSTORED AS PARQUET TBLPROPERTIES ('transactional'='false');")
       query_file.close()          
       
       #ejecutar el sql
       self.executeFolder(self.getSQLPath() + \
        type(self).__name__, self.obtener_params())
    
    def run_feature_pipeline(self):
        
        from pathlib import Path     
        
        DEFAULT_PIPELINE_PATH = os.path.join(os.path.dirname(__file__), "pipeline.py")    
         
        # default config inside package
        params = self.obtener_params()
        
        default_config = os.path.join(os.path.dirname(__file__), "config.yaml")       
        config_path = os.path.join(os.getcwd(), params.get('config_file')) if params.get('config_file') is True else default_config
        config_path = str(Path(config_path).resolve())

        if not os.path.exists(config_path):
            print(f"Config file not found: {config_path}")
            sys.exit(2)
        
        spec = importlib.util.spec_from_file_location("pipeline", DEFAULT_PIPELINE_PATH)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        module.build_and_save_features(config_path)  
        
    #leer parquet y generar query    
    def parquet_to_lz(self):        
           
        params = self.getGlobalConfiguration()["parametros_lz"]
        
        default_config = os.path.join(os.path.dirname(__file__), "config.yaml")       
        config_path = os.path.join(os.getcwd(), params.get('config_file')) if params.get('config_file') is True else default_config
       
        cfg = self.load_config(config_path)       
        out_dir = cfg.get('output_dir', 'src/vsti_vapi_modelo_predictivo_apis_dev/static/feature_pipeline_output')
       
        #leer el parquet
        parquet_path = os.path.join(os.getcwd(),out_dir)        
               
        parquet = os.path.join(parquet_path, 'features.parquet')
        df_parquet = pd.read_parquet(parquet)        
        for index, row in df_parquet.iterrows():
            values = [str(value).replace("'", "") for value in row.values]  
            values = ", ".join(f"'{value}'" for value in values)  
            insert_query = f"INSERT INTO {params.get('zona_procesamiento')}.{params.get('prefijo')}{params.get('nombre_tabla')} VALUES ({values})"           
            self.helper.ejecutar_consulta(insert_query)