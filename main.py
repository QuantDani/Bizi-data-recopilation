import pandas as pd
import time
from datetime import datetime
import requests
import time
import random
import os
import logging
import json

# cargamos las variables de entorno
from dotenv import load_dotenv
load_dotenv()

# configuramos el logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename='app.log'
)

from bizi import create_dataframe, scrap_web
from weather import create_tiempo_file, get_weather_data
from auxiliar import send_message


if __name__ == "__main__":
    CSV_FILE = "data.csv"
    JSON_FILE = "tiempo.jsonl"
    DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"


    # cargamos como sera el multiinidce del df
    lista_estaciones = [f"station_{id}" for id in pd.read_csv("estaciones.csv")["Id"].to_list()]

    tipos_dato = ['bikes_available', 'bikes_disabled', 'docks_available']

    column_multi_index = pd.MultiIndex.from_product(
        [lista_estaciones, tipos_dato],
        names=['Stations', 'Data']
    )

    INDEX_NAME = "timestamp"
    
    # numero total de registros que harmeos cada dia
    total_registros = 0

    while True:
        time.sleep(1)
        now = datetime.now()
        # esperamos al incio de cada minuto
        if now.second != 0:
            continue
        
        # si son las 24:00 mandamos el mensaje diario de actividad del bot
        if now.hour == 0 and now.minute == 0:
            send_message(f"Bot activo.\nNumero de registros de hoy: {total_registros}")
            total_registros = 0

        # comprobamos que exista el csv, si no lo creamos
        if not os.path.exists(CSV_FILE):
            create_dataframe(CSV_FILE)

        # comprobamos que exista el jsonl para el clima, si no lo creamos
        if not os.path.exists(JSON_FILE):
            create_tiempo_file(JSON_FILE)
        
        # si el minuto es multiplo de 15, guardamos los datos meteorologicos
        if now.minute % 15 == 0:
            get_weather_data(JSON_FILE)

        # tomamos los datos de este minuto scrapeando la web
        new_data = scrap_web()
        
        if not new_data:
            print("No se han registrado datos para el momento: ", now.strftime(DATETIME_FORMAT))
            continue

        # si tenemos datos, los añadimos al dataframe
        try:
            df_nueva_fila = pd.DataFrame(
                [new_data],        
                index=[now],       
                columns=column_multi_index   
                )
            
            df_nueva_fila.index.name = INDEX_NAME

            file_exists = os.path.exists(CSV_FILE)

            df_nueva_fila.to_csv(
                CSV_FILE,
                mode='a',  # añadir
                header=not file_exists, # escribimos cabecera solo si el df no existe
                date_format=DATETIME_FORMAT
            )
            
            logging.info(f"Registro para {now.strftime(DATETIME_FORMAT)} creado con exito")

            # añadimos un nuevo registro
            total_registros += 1

        except Exception as e:
            msj_error = f"Un error inesperado ocurrió leyendo / guardando el dataframe: {e}" 
            logging.critical(msj_error,exc_info=True) # exc_info=True añade el traceback
            send_message(msj_error)

        
        
            

        