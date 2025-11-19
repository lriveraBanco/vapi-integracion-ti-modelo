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
-- Descripción: Script de ejecución de la rutina
-----------------------------------------------------------------------------
-----------------------------------------------------------------------------
"""
from etl import ExtractTransformLoad
from orquestador2.orquestador2     import Orchestrator
import argparse
import os


parser = argparse.ArgumentParser(description='Orq 2 vapi modelo predictivo apis dev')
parser.add_argument('-y', '--kwargs_year' ,
                    type = int, help = 'Año de ejecución')
parser.add_argument('-m', '--kwargs_month',
                    type = int, help = 'Mes de ejecución')
parser.add_argument('-d', '--kwargs_day',
                    type = int, help = 'Día de ejecución')
parser.add_argument('-zp', '--zona_procesamiento',
                    type = str, help = 'Zona de procesamiento de la rutina')
parser.add_argument('-lt', '--log_type',
                    type = str, help = 'Tipo de log: normal, compilación o estabilidad',
                    default = '')
parser.add_argument('-pl', '--porcentaje_limit',
                    type = int, help = 'Porcentaje para el LIMIT en los logs de compilación',
                    default = 100)
parser.add_argument('-f', '--config_file',
                    type = str, help = 'Archivo de configuracion',
                    default = 100)


args = parser.parse_args()
kw = {k:w for k,w in vars(args).items() if w}
kw["log_type"] = args.log_type
kw["porcentaje_limit"] = args.porcentaje_limit


if kw["log_type"] in ["cmp", "est"]:
    if not os.path.exists(os.path.join(os.getcwd(), "src")):
        print("No se encontró carpeta src, por lo tanto " \
            "se asume que no está en la carpeta de trabajo " \
            "adecuada para la generación de logs de calendarización")
        exit()
    logs_path = os.path.join(os.getcwd(), "logs_calendarizacion")
    if kw["log_type"] == "cmp":
        percent_flag = False
        for i in range(100):
            if kw["porcentaje_limit"] == (i + 1):
                percent_flag = True
                break
        if not percent_flag:
            print("El porcentaje_limit debe ser un valor entero " \
                "entre 1 y 100. Adicionalmente, es un parámetro "
                "obligatorio para logs de compilación 'cmp'")
            exit()
else:
    logs_path = os.path.join(os.getcwd(), "logs")
    del kw["log_type"]
    del kw["porcentaje_limit"]
kw["log_path"] = logs_path


if not os.path.exists(logs_path):
    print("No existía la carpeta de logs para calendarización, se está creando.")
    os.mkdir(logs_path)


steps = [
    ExtractTransformLoad(**kw)
]


def main():
    orquestador = Orchestrator(
        "Orq 2 vapi modelo predictivo apis dev" \
        , steps
        , **kw)
    orquestador.ejecutar()


if __name__ == '__main__':
    main()
