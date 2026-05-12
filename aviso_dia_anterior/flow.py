from datetime import datetime
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import smtplib

from prefect import flow, task
from consulterscommons.log_tools.prefect_log_config import PrefectLogger
from consulterscommons.config_tools.prefect_tools import read_variable, read_secret

# CONFIG
LOGGER_GLOBAL = PrefectLogger(__file__)

MAIL_SERVER = "smtp.gmail.com"
MAIL_PORT = 587
MAIL_USER = "tomas.gallo@consulters.com.ar"
MAIL_PASSWORD = read_secret("claveemail")

#DESTINATARIOS = ["tomas.gallo@consulters.com.ar", "priscila.scharf@consulters.com.ar"]
DESTINATARIOS = ["gmacho@favicur.com.ar", "daguero@favicur.com.ar", "tomas.gallo@consulters.com.ar", "priscila.scharf@consulters.com.ar"]

LINK_CARPETA = "https://drive.google.com/drive/folders/18xN7H-ocrXce9w-2lTOicnEgdoUAp2S_"


@task
def enviar_recordatorio_dia_anterior():
    logger = LOGGER_GLOBAL.obtener_logger_prefect()

    msg = MIMEMultipart()
    msg["From"] = MAIL_USER
    msg["To"] = ", ".join(DESTINATARIOS)
    msg["Subject"] = "Recordatorio carga de archivos reporte Pago a Proveedores"

    body = f"""
Buenas tardes,

No olviden subir los archivos al Drive:
{LINK_CARPETA}

Recuerden que si no suben los archivos, el reporte se enviará desactualizado

Saludos
"""

    msg.attach(MIMEText(body, "plain"))

    server = smtplib.SMTP(MAIL_SERVER, MAIL_PORT)
    server.starttls()
    server.login(MAIL_USER, MAIL_PASSWORD)
    server.sendmail(MAIL_USER, DESTINATARIOS, msg.as_string())
    server.quit()

    logger.info("📧 Recordatorio día anterior enviado")


@flow(name="Recordatorio Día Anterior - Pago Proveedores")
def main_flow():
    enviar_recordatorio_dia_anterior()


if __name__ == "__main__":
    main_flow()