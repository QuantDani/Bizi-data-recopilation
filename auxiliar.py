import requests
import logging
import os

def send_message(msg: str) -> None:
    TOKEN = os.environ.get("TELEGRAM_TOKEN")
    CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID") 
    
    URL_TELEGRAM = f"https://api.telegram.org/bot{TOKEN}/sendMessage"

    payload = {
        'chat_id': CHAT_ID,
        'text': msg,
        'parse_mode': 'Markdown'
    }
    
    try:
        respuesta = requests.get(URL_TELEGRAM, params=payload)
        
        if respuesta.status_code == 200:
            logging.info(f"Mensaje enviado con éxito al chat {CHAT_ID}")
        else:
            logging.error(f"Error al enviar: {respuesta.status_code} - {respuesta.text}")
            
    except Exception as e:
        logging.error(f"Ocurrió un error al enviar el mensaje: {e}")