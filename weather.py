import requests
import json
import datetime
from auxiliar import send_message
import logging
import os


def create_tiempo_file(LOG_FILE: str):
    with open(LOG_FILE,"w",encoding = "utf-8") as f:
        pass
    f.close()

def get_weather_data(LOG_FILE: str):
    METEO_API_KEY = os.environ.get("METEO_API_KEY")
    API_URL = f"https://www.meteosource.com/api/v1/free/point?place_id=zaragoza&sections=current%2Chourly&language=en&units=metric&key={METEO_API_KEY}"

    DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"

    try:
        response = requests.get(API_URL,timeout = 10)
        
        response.raise_for_status() 
        
        data = response.json()
        
        # añadimos el tiempo actual
        data['request_timestamp'] = datetime.datetime.now().strftime(format = DATETIME_FORMAT)
        
        # lo guardamos en el fichero
        with open(LOG_FILE, 'a', encoding='utf-8') as f:
            json_line = json.dumps(data)
            
            # Escribimos esa línea y un salto de línea
            f.write(json_line + '\n')
        f.close()
            
        logging.info(f"Dato meteorologico de las {data['request_timestamp']} guardado correctamente.")

    except requests.exceptions.HTTPError as e:
        msj_error = f"Error HTTP (ej. 404, 503) meteorologicos: {e}"
        logging.error(msj_error)
        send_message(msj_error)
    except requests.exceptions.ConnectionError as e:
        msj_error = f"Error de conexión (ej. DNS, red caída) meteorologicos: {e}"
        logging.error(msj_error)
        send_message(msj_error)
    except requests.exceptions.Timeout:
        msj_error = f"Timeout: La petición a {API_URL} tardó demasiado."
        logging.error(msj_error)
        send_message(msj_error)
    except requests.exceptions.RequestException as e:
        # captura cualquier otro error de requests
        msj_error = f"Error general de requests meteorologica: {e}"
        logging.error(msj_error)
        send_message(msj_error) 
    except json.JSONDecodeError:
        # guardar el texto de la respuesta puede ayudar a depurar
        msj_error = f"Error al decodificar JSON en datos meteorologicos. Respuesta recibida (primeros 200 chars): {response.text[:200]}..."
        logging.error(msj_error)
        send_message(msj_error)
    except Exception as e:
        msj_error = f"Un error inesperado ocurrió: {e}" 
        logging.critical(msj_error,exc_info=True) # exc_info=True añade el traceback
        send_message(msj_error)