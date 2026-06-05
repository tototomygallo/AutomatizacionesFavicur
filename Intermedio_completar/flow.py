import os
import pandas as pd
from datetime import datetime
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
from prefect import flow, task
from consulterscommons.log_tools.prefect_log_config import PrefectLogger
from consulterscommons.config_tools.prefect_tools import read_secret

# --- CONFIGURACIÓN DE RUTAS ---
RUTA_DRIVE = '/home/favidw/gdrive/Pago a Proveedores'
LOGGER_GLOBAL = PrefectLogger(__file__)
ARCHIVO_MAESTRO_PAGOS = 'Cruce_intermedio_Pagos.xlsx'
RUTA_LOCAL_TEST = '/home/favidw/favicur/automatizaciones/Python'

# --- CONFIGURACIÓN DE MAIL ---
"""
DESTINATARIOS = [
    "tomas.gallo@consulters.com.ar", 
    "gmacho@favicur.com.ar", 
    "daguero@favicur.com.ar", 
    "priscila.scharf@consulters.com.ar", 
    "jpinones@favicur.com.ar", 
    "ignacio@favicur.com.ar"
]
"""
#DESTINATARIOS=["tomas.gallo@consulters.com.ar"]
DESTINATARIOS = ["gmacho@favicur.com.ar", "daguero@favicur.com.ar", "tomas.gallo@consulters.com.ar", "priscila.scharf@consulters.com.ar", "jpinones@favicur.com.ar", "ignacio@favicur.com.ar"]

MAIL_SERVER = "smtp.gmail.com"
MAIL_PORT = 587
MAIL_USER = "tomas.gallo@consulters.com.ar"
MAIL_SERVER_PASSWORD = read_secret("claveemail")

def limpiar_monto_apex(serie):
    return pd.to_numeric(serie, errors='coerce').fillna(0)

@task
def generar_cruce_proveedores() -> str:
    logger = LOGGER_GLOBAL.obtener_logger_prefect()
    logger.info("Iniciando cruce intermedio de APEX y Odoo (9:40 AM)...")
    
    # 1. Cargar fuentes originales
    df_apex = pd.read_excel(os.path.join(RUTA_DRIVE, 'Vencimientos de proveedores.xlsx'))
    df_odoo = pd.read_excel(os.path.join(RUTA_DRIVE, 'Proveedores a pagar - Odoo.xlsx'))
    df_odoo = df_odoo.rename(columns={'Referencia de la orden': 'Orden de compra'})
    
    print(df_apex.columns)
    print(df_odoo.columns)
    
    # 👇 NUEVO: Detectar dinámicamente la columna de términos en Odoo para evitar fallos de tildes
    col_terminos_odoo = [
        c for c in df_odoo.columns 
        if 'PAGO' in str(c).upper() and ('MIN' in str(c).upper() or 'TERM' in str(c).upper())
    ]
    
    # Columnas base indispensables para el merge
    columnas_odoo_seleccionadas = ['Orden de compra', 'Método de pago']
    
    if col_terminos_odoo:
        nombre_col_terminos = col_terminos_odoo[0]
        columnas_odoo_seleccionadas.append(nombre_col_terminos)
        logger.info(f"🔍 Se arrastrará la columna de términos detectada: '{nombre_col_terminos}'")
    else:
        logger.warning("⚠️ No se detectó ninguna columna de términos en el archivo de Odoo.")

    # 2. Realizar el merge incluyendo la columna de términos si existe
    df_p = pd.merge(df_apex, df_odoo[columnas_odoo_seleccionadas], on='Orden de compra', how='left')
    df_p['Saldo'] = limpiar_monto_apex(df_p['Saldo'])
    df_p['Método de pago'] = df_p['Método de pago'].fillna('#N/D').astype(str)
    
    # 3. Guardar el archivo maestro en local temporalmente
    path_destino = os.path.join(RUTA_LOCAL_TEST, ARCHIVO_MAESTRO_PAGOS)
    df_p.to_excel(path_destino, index=False)
    
    logger.info(f"✅ Archivo maestro generado con éxito en: {path_destino}")
    return path_destino

@task
def enviar_mail_intermedio(path_adjunto: str):
    logger = LOGGER_GLOBAL.obtener_logger_prefect()
    logger.info("Preparando envío de correo con archivo intermedio...")
    
    msg = MIMEMultipart()
    msg["From"] = MAIL_USER
    msg["To"] = ", ".join(DESTINATARIOS)
    msg["Subject"] = "Cruce Intermedio de Pagos a Proveedores - FAVICUR"

    # 👇 Cuerpo solicitado para la revisión manual
    cuerpo = (
        "Hola,\n\n"
        "Se ha generado el cruce intermedio de pagos.\n"
        "Por favor, completar de ser necesario los campos sin método de pago asociado antes del proceso final.\n\n"
        "Saludos."
    )
    msg.attach(MIMEText(cuerpo, 'plain'))

    # Adjuntar archivo Excel
    with open(path_adjunto, "rb") as f:
        part = MIMEBase("application", "octet-stream")
        part.set_payload(f.read())
        encoders.encode_base64(part)
        part.add_header("Content-Disposition", f"attachment; filename={os.path.basename(path_adjunto)}")
        msg.attach(part)

    # Conexión al servidor SMTP
    server = smtplib.SMTP(MAIL_SERVER, MAIL_PORT)
    server.starttls()
    server.login(MAIL_USER, MAIL_SERVER_PASSWORD)
    server.sendmail(MAIL_USER, DESTINATARIOS, msg.as_string())
    server.quit()
    logger.info("📧 Mail intermedio enviado con éxito.")

@flow(name="Favicur - 1. Generar Cruce de Pagos")
def flujo_intermedio_940():
    logger = LOGGER_GLOBAL.obtener_logger_prefect()
    try:
        # Corre la generación del archivo y guarda la ruta devuelta
        ruta_archivo = generar_cruce_proveedores()
        # Envía el correo usando la ruta del archivo generado
        enviar_mail_intermedio(ruta_archivo)
    except Exception as e:
        logger.error(f"Fallo en el procesamiento intermedio: {e}")

if __name__ == "__main__":
    flujo_intermedio_940()