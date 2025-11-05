import pandas as pd
from datetime import datetime
import requests
import random
import logging
from auxiliar import send_message
import json

def create_dataframe(file_path: str) -> None:
    """
    Esta funcion crea el dataframe donde guardaremos es estado del sistema Bizi
    por cada minuto. El dataframe es un dataframe multiindice.

    Args:
        file_path (str): La ruta al archivo donde guardo el dataframe
    Returns:
        None
    """

    lista_estaciones = [f"station_{id}" for id in pd.read_csv("estaciones.csv")["Id"].to_list()]

    tipos_dato = ['bikes_available', 'bikes_disabled', 'docks_available']

    column_multi_index = pd.MultiIndex.from_product(
        [lista_estaciones, tipos_dato],
        names=['Stations', 'Data']
    )

    df = pd.DataFrame(columns=column_multi_index, index=pd.DatetimeIndex([])) # el index sera un dataframe
    
    df.to_csv(file_path)

def scrap_web():
    url = "https://zaragoza.publicbikesystem.net/customer/gbfs/v2/en/station_status"
    user_agents_list = [
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Firefox/108.0',
        'Mozilla/5.0 (X11; Linux x86_64; rv:108.0) Gecko/20100101 Firefox/108.0'
    ]
    user_agent_aleatorio = random.choice(user_agents_list)
    headers = {
        'User-Agent': user_agent_aleatorio,
        'Referer': 'https://www.google.com/'
    }

    try:
        # hacemos la peticion
        response = requests.get(url, headers=headers, timeout=10) # timeout de 10 segundos
        
        #  raise_for_status() por si hay algun error HTTP
        response.raise_for_status() 
        
        # cargamos en json
        data = response.json()

        
        new_data = {}
        
        # .get('data', {}) devuelve un dict vacío si 'data' no existe
        stations_list = data.get('data', {}).get('stations', [])

        if not stations_list:
            # si la lista vino vacia, registramos

            msj_error = f"No se encontraron estaciones en la respuesta de {url}"
            logging.error(msj_error)
            send_message(msj_error)
            
            return {}
        
        for station in stations_list:
            station_id_raw = station['station_id']
            station_id = f"station_{station_id_raw}"
            
            new_data[(station_id, 'bikes_available')] = station['num_bikes_available'] 
            new_data[(station_id, 'bikes_disabled')] = station['num_bikes_disabled'] 
            new_data[(station_id, 'docks_available')] = station['num_docks_available']

        logging.info(f"Scraping exitoso. Se procesaron {len(stations_list)} estaciones.")
        
        return new_data

    # excepciones
    except requests.exceptions.HTTPError as e:
        msj_error = f"Error HTTP (ej. 404, 503): {e}"
        logging.error(msj_error)
        send_message(msj_error)
    except requests.exceptions.ConnectionError as e:
        msj_error = f"Error de conexión (ej. DNS, red caída): {e}"
        logging.error(msj_error)
        send_message(msj_error)
    except requests.exceptions.Timeout:
        msj_error = f"Timeout: La petición a {url} tardó demasiado."
        logging.error(msj_error)
        send_message(msj_error)
    except requests.exceptions.RequestException as e:
        # captura cualquier otro error de requests
        msj_error = f"Error general de requests: {e}"
        logging.error(msj_error)
        send_message(msj_error)
    
    except json.JSONDecodeError:
        # guardar el texto de la respuesta puede ayudar a depurar
        msj_error = f"Error al decodificar JSON. Respuesta recibida (primeros 200 chars): {response.text[:200]}..."
        logging.error(msj_error)
        send_message(msj_error)
    
    # capturar explícitamente errores de procesamient
    except (KeyError, TypeError) as e:
        msj_error = f"Error al procesar la estructura de datos: {e}"
        logging.error(msj_error)
        send_message(msj_error)
    
    # una excepción genérica para cualquier otra cosa
    except Exception as e:
        msj_error = f"Un error inesperado ocurrió: {e}" 
        logging.critical(msj_error,exc_info=True) # exc_info=True añade el traceback
        send_message(msj_error)

    # Devolver un valor consistente en caso de error (dict vacío suele ser mejor que None)
    return {}
