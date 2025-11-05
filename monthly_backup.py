import boto3
import os
import datetime
import logging
import sys

# cargamos las variables de entorno
from dotenv import load_dotenv
load_dotenv() 

# intentamos importar send_message
try:
    from auxiliar import send_message
except ImportError:
    # fallback por si la función no está o falla
    def send_message(message):
        print(f"Intento de enviar mensaje: {message} (send_message no importado)")

# Configurar el logging para que escriba en la consola
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    stream=sys.stdout
)

# --- Configuración ---
BUCKET_NAME = "bizi-tiempo-mensual-bucket"
FILES_TO_BACKUP = [
    'tiempo.jsonl',
    'data.csv',
    'app.log'
]
# ---------------------

logging.info("--- Iniciando backup mensual ---")

# Crea el cliente de S3
s3_client = boto3.client('s3')

last_month = datetime.date.today().replace(day=1) - datetime.timedelta(days=1)
timestamp_str = last_month.strftime("%Y-%m")

archivos_fallidos = 0

for local_file in FILES_TO_BACKUP:
    if not os.path.exists(local_file):
        logging.warning(f"AVISO: No se encontró el fichero {local_file}. Saltando...")
        send_message(f"Durante el backup mensual, no se ha encontrado el archivo: {local_file}")
        continue

    base, ext = os.path.splitext(local_file)
    new_local_name = f"{base}-{timestamp_str}{ext}"
    s3_key = f"logs/{timestamp_str}/{new_local_name}"
    
    try:
        logging.info(f"Renombrando {local_file} a {new_local_name}...")
        os.rename(local_file, new_local_name)

        logging.info(f"Subiendo {new_local_name} a S3 (Bucket: {BUCKET_NAME})...")
        s3_client.upload_file(new_local_name, BUCKET_NAME, s3_key)
        
        logging.info(f"¡Subida completada! s3://{BUCKET_NAME}/{s3_key}")

        os.remove(new_local_name)
        logging.info(f"Fichero local {new_local_name} borrado.")

    except Exception as e:
        archivos_fallidos += 1
        logging.error(f"Error procesando {local_file}: {e}", exc_info=True)
        # Si algo falló, renombra el fichero de vuelta
        if os.path.exists(new_local_name):
            os.rename(new_local_name, local_file)
            logging.info(f"¡Backup fallido! Fichero restaurado a {local_file}")

# --- Reporte final por Telegram ---
if archivos_fallidos == 0:
    mensaje_final = f"Backup mensual completado con éxito para {timestamp_str}."
    logging.info(mensaje_final)
else:
    mensaje_final = f"Backup mensual para {timestamp_str} falló. {archivos_fallidos} archivo(s) no se pudieron procesar."
    logging.error(mensaje_final)

try:
    send_message(mensaje_final)
except Exception as e:
    logging.error(f"Falló el envío del mensaje de Telegram: {e}")

logging.info("--- Backup mensual terminado ---")