import pandas as pd
import numpy as np
from datetime import datetime
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
from typing import List, Tuple
import smtplib
from prefect import flow, task
from consulterscommons.log_tools.prefect_log_config import PrefectLogger
from consulterscommons.config_tools.prefect_tools import read_variable, read_secret

import os # Nueva librería para manejar rutas de carpetas

# --- CONFIGURACIÓN DE RUTAS ---
# REEMPLAZA ESTO: Copia la ruta de la carpeta de Drive desde tu explorador de archivos
#RUTA_DRIVE = r'G:\.shortcut-targets-by-id\18xN7H-ocrXce9w-2lTOicnEgdoUAp2S_\Pago a Proveedores'
RUTA_DRIVE = '/home/favidw/gdrive/Pago a Proveedores'
# --- CONFIGURACIÓN ---
LIMITE_TRANSFERENCIA = 300000




# --- CONFIGURACIÓN DE RUTAS Y PARÁMETROS ---
LOGGER_GLOBAL = PrefectLogger(__file__)
LIMITE_TRANSFERENCIA = 300000
DESTINATARIOS = ["tomas.gallo@consulters.com.ar", "priscila.scharf@consulters.com.ar"]
#DESTINATARIOS = ["gmacho@favicur.com.ar", "daguero@favicur.com.ar", "tomas.gallo@consulters.com.ar", "priscila.scharf@consulters.com.ar"]

# CONFIG MAIL (Desde Prefect Secrets)
MAIL_SERVER = "smtp.gmail.com"
MAIL_PORT = 587
MAIL_USER = "tomigallok@gmail.com"
MAIL_SERVER_PASSWORD = "dnkp znst iebs wvwo"
MAIL_SERVER_PASSWORD=read_secret("claveemail")

def limpiar_monto_contable(serie):
    return pd.to_numeric(serie.astype(str).str.replace('.', '', regex=False).str.replace(',', '.', regex=False), errors='coerce').fillna(0)

def limpiar_monto_apex(serie):
    return pd.to_numeric(serie, errors='coerce').fillna(0)

def limpiar_monto_valores(serie):
    return pd.to_numeric(serie.astype(str).str.replace(',', '', regex=False), errors='coerce').fillna(0)
# --- AJUSTE EN LA CARGA DE VALORES POR FECHAS ---

def procesar_valores_por_fecha(df, columna_fecha='Fecha Vto.', columna_importe='Importe'):
    # Aseguramos que la fecha sea formato datetime
    df[columna_fecha] = pd.to_datetime(df[columna_fecha], errors='coerce')
    df[columna_importe] = limpiar_monto_valores(df[columna_importe])
    
    # Obtenemos el número de día de la semana (0=Lunes, 2=Miércoles, 4=Viernes)
    # Importante: Esto depende de qué "semana" estemos hablando. 
    # Generalmente comparamos contra la fecha de hoy.
    
    hoy = pd.Timestamp.now()
    # Miércoles de esta semana
    miercoles = hoy + pd.Timedelta(days=(2 - hoy.dayofweek) % 7)
    # Viernes de esta semana
    viernes = hoy + pd.Timedelta(days=(4 - hoy.dayofweek) % 7)
    # Lunes de la semana que viene
    lunes_prox = hoy + pd.Timedelta(days=(7 - hoy.dayofweek) % 7)

    vto_miercoles = df[df[columna_fecha] <= miercoles][columna_importe].sum()
    vto_viernes = df[(df[columna_fecha] > miercoles) & (df[columna_fecha] <= viernes)][columna_importe].sum()
    vto_lunes = df[df[columna_fecha] >= lunes_prox][columna_importe].sum()
    
    return vto_miercoles, vto_viernes, vto_lunes



print("🚀 Iniciando Proceso Integral de Pagos y Disponibilidad...")


@task(retries=2)
def verificar_fuentes_actualizadas(archivos: List[str]) -> Tuple[bool, List[str]]:
    """Verifica si los archivos del Drive se modificaron hoy."""
    hoy = datetime.now().date()
    faltantes = []
    for arc in archivos:
        path = os.path.join(RUTA_DRIVE, arc)
        if not os.path.exists(path):
            faltantes.append(f"{arc} (No existe)")
            continue
        mtime = datetime.fromtimestamp(os.path.getmtime(path)).date()
        if mtime < hoy:
            faltantes.append(arc)
    return len(faltantes) == 0, faltantes



@task
def generar_reporte_excel():
    # 1. CARGAR DATOS DE PAGOS
    df_apex = pd.read_excel(os.path.join(RUTA_DRIVE, 'Vencimientos de proveedores.xlsx'))
    df_odoo = pd.read_excel(os.path.join(RUTA_DRIVE, 'Proveedores a pagar - Odoo.xlsx'))
    df_odoo = df_odoo.rename(columns={'Referencia de la orden': 'Orden de compra'})

    df_p = pd.merge(df_apex, df_odoo[['Orden de compra', 'Método de pago']], on='Orden de compra', how='left')
    df_p['Saldo'] = limpiar_monto_apex(df_p['Saldo'])
    df_p['Método de pago'] = df_p['Método de pago'].fillna('#N/D').astype(str)




    # Definimos qué métodos están sujetos a "convertirse" en transferencia si son montos chicos
    # (Agregá acá los que Gabriel ignoraría si son < 300k)
    metodos_convertibles = ['Echeq de terceros', 'Cheques CBA', 'Cheques BA']

    # Aplicamos la condición: 
    # Si (el Saldo es < 300k) Y (el Método actual está en nuestra lista de convertibles)
    df_p.loc[
        (df_p["Saldo"] < LIMITE_TRANSFERENCIA) & 
        (df_p["Método de pago"].isin(metodos_convertibles)), 
        'Método de pago'
    ] = 'Transferencia'

    # Luego, como ya hacías, pasamos las transferencias a Galicia
    df_p.loc[df_p['Método de pago'] == 'Transferencia', 'Método de pago'] = 'Galicia'


    # 2. CARGAR VALORES (Drive)
    df_echeqs = pd.read_excel(os.path.join(RUTA_DRIVE, 'eCheqs.xlsx'))
    df_cba = pd.read_excel(os.path.join(RUTA_DRIVE, 'Cheques Físicos CBA.xlsx'))
    df_bsas = pd.read_excel(os.path.join(RUTA_DRIVE, 'Cheques Físicos BS AS.xlsx'))

    total_echeqs = limpiar_monto_valores(df_echeqs['Importe']).sum()
    total_cba = limpiar_monto_valores(df_cba['Importe']).sum()
    total_bsas = limpiar_monto_valores(df_bsas['Importe']).sum()

    # 3. CARGAR SALDOS CONTABLES Y FCI
    #
    df_cc = pd.read_csv(os.path.join(RUTA_DRIVE, 'Cuentas Contables.csv'), sep=';', encoding='latin-1')
    df_fci = pd.read_csv(os.path.join(RUTA_DRIVE, 'Cuentas Contables (1).csv'), sep=';', encoding='latin-1')
    df_cc['Saldo fecha'] = limpiar_monto_contable(df_cc['Saldo fecha'])
    df_fci['Saldo fecha'] = limpiar_monto_contable(df_fci['Saldo fecha'])

    # 4. CONSTRUIR TABLERO (TABLA 2)
    filas_tablero = [
        'Mercado Pago', 'Bancor', 'Nación', 'ICBC', 'Patagonia', 'Macro', 'Credicoop', 'Comafi', 'Galicia',
        'Becerra/Balanz', 'TSA', 'Efectivo', 'Echeq de terceros', 'Cheques CBA', 'Cheques BA'
    ]
    columnas = ['proveedores pendientes', 'otros pagos pendientes', 'saldo online', 'saldo-pagos', 'pendientes de acreditacion', 'FCI', 'saldo con pendientes y FCI']

    df_tablero = pd.DataFrame(index=filas_tablero, columns=columnas).fillna(0.0)

    # A. Saldo Online (Bancos)
    mapa_bancos = {
        'Bancor': 'PCIA DE CORDOBA', 'Nación': 'NACION', 'ICBC': 'ICBC', 
        'Patagonia': 'PATAGONIA', 'Macro': 'MACRO', 'Galicia': 'GALICIA', 
        'Credicoop': 'CREDICOOP', 'Comafi': 'COMAFI'
    }
    for b, term in mapa_bancos.items():
        saldo = df_cc[df_cc['Descripcion'].str.contains(term, na=False)]['Saldo fecha'].sum()
        df_tablero.at[b, 'saldo online'] = saldo





    # C. Proveedores Pendientes (Mapeo de TODOS los métodos)
    # Sumamos del detalle de pagos agrupado por el nombre del banco/método
    resumen_pagos = df_p.groupby('Método de pago')['Saldo'].sum()
    for metodo, monto in resumen_pagos.items():
        if metodo in df_tablero.index:
            df_tablero.at[metodo, 'proveedores pendientes'] = monto

    # D. FCI
    mapa_fci = {'Macro': 'MACRO', 'Credicoop': 'CREDICOOP', 'Patagonia': 'PATAGONIA', 'Bancor': 'CORDOBA', 'Galicia': 'GALICIA', 'Comafi': 'COMAFI'}
    for b, term in mapa_fci.items():
        fci_valor = df_fci[df_fci['Descripcion'].str.contains(term, na=False)]['Saldo fecha'].sum()
        df_tablero.at[b, 'FCI'] = fci_valor




    # 2. CARGAR Y CLASIFICAR VALORES
    # Aplicamos la función a cada archivo (Asegurate que el nombre de la columna sea el correcto)
    e_m, e_v, e_l = procesar_valores_por_fecha(df_echeqs)
    cba_m, cba_v, cba_l = procesar_valores_por_fecha(df_cba)
    bsas_m, bsas_v, bsas_l = procesar_valores_por_fecha(df_bsas)

    # 4. CONSTRUIR TABLERO (Actualizando las celdas específicas)

    # Fila Echeq de terceros
    df_tablero.at['Echeq de terceros', 'saldo online'] = e_m
    df_tablero.at['Echeq de terceros', 'pendientes de acreditacion'] = e_v
    df_tablero.at['Echeq de terceros', 'FCI'] = e_l

    # Fila Cheques CBA
    df_tablero.at['Cheques CBA', 'saldo online'] = cba_m
    df_tablero.at['Cheques CBA', 'pendientes de acreditacion'] = cba_v
    df_tablero.at['Cheques CBA', 'FCI'] = cba_l

    # Fila Cheques BA
    df_tablero.at['Cheques BA', 'saldo online'] = bsas_m
    df_tablero.at['Cheques BA', 'pendientes de acreditacion'] = bsas_v
    df_tablero.at['Cheques BA', 'FCI'] = bsas_l



    # --- E. CÁLCULOS AUTOMÁTICOS (CORREGIDO) ---

    # 1. Calculamos saldo disponible real (Saldo Online + Otros Pagos si existieran)
    # Restamos lo que debemos pagar
    df_tablero['saldo-pagos'] = df_tablero['saldo online'] - df_tablero['proveedores pendientes'] - df_tablero['otros pagos pendientes']

    # 2. El saldo final debe considerar la cadena de tiempo:
    # Saldo con pendientes y FCI = (Saldo hoy - Pagos hoy) + Pendientes Viernes + FCI (Lunes)
    df_tablero['saldo con pendientes y FCI'] = (
        df_tablero['saldo-pagos'] + 
        df_tablero['pendientes de acreditacion'] + 
        df_tablero['FCI']
    )
    # Fila de TOTAL
    df_tablero.loc['TOTAL'] = df_tablero.sum()

    # 5. EXPORTACIÓN COMPLETA
    print("\n" + "="*40)
    print("TABLA 1: TOTALES POR MÉTODO")
    print("="*40)
    resumen_metodos = df_p.groupby('Método de pago')['Saldo'].sum().reset_index()
    print(resumen_metodos.to_string(index=False, float_format="${:,.2f}".format))

    # --- 5. EXPORTACIÓN CON DISEÑO AVANZADO ---
    df_p = df_p.fillna('')
    nombre_archivo = f'Reporte_Pagos_{datetime.now().strftime("%Y%m%d")}.xlsx'
    with pd.ExcelWriter(nombre_archivo, engine='xlsxwriter') as writer:
        # 1. Escribir Detalle_Pagos (Tabla Izquierda)
        df_p.to_excel(writer, sheet_name='Tablero Final', index=False, startcol=0)
        
        # 2. Escribir Tablero_Control (Tabla Derecha - Amarillo Pastel)
        col_inicio_tablero = len(df_p.columns) + 1
        df_tablero.to_excel(writer, sheet_name='Tablero Final', startcol=col_inicio_tablero)

        workbook  = writer.book
        worksheet = writer.sheets['Tablero Final']

        # FORMATOS
        fmt_azul = workbook.add_format({'bg_color': '#1F4E78', 'font_color': 'white', 'bold': True, 'border': 1})
        fmt_verde = workbook.add_format({'bg_color': "#2DAC62", 'font_color': 'white', 'bold': True, 'border': 1})
        
        fmt_blanco = workbook.add_format({'bg_color': "#FFFFFF", 'border': 1, 'num_format': '#,##0.00'})
        fmt_amarillo_pastel = workbook.add_format({'bg_color': "#FFEEBB", 'border': 1, 'num_format': '#,##0.00'})
        fmt_azul_pastel = workbook.add_format({'bg_color': "#BDD4E7", 'border': 1, 'num_format': '#,##0.00'})
        fmt_gris_total = workbook.add_format({'bg_color': '#D9D9D9', 'bold': True, 'border': 1, 'num_format': '#,##0.00'})
        fmt_negativo = workbook.add_format({'font_color': '#9C0006', 'bg_color': '#FFC7CE'}) # Rojo suave para negativos
        fmt_moneda = workbook.add_format({'num_format': '#,##0.00', 'border': 1})

        # Pintar encabezados tabla detalle
        for col_num, value in enumerate(df_p.columns.values):
            worksheet.write(0, col_num, value, fmt_azul)

        # Pintar encabezados tablero
        worksheet.write(0, col_inicio_tablero, "Concepto", fmt_verde)
        for col_num, value in enumerate(df_tablero.columns.values):
            worksheet.write(0, col_inicio_tablero + col_num + 1, value, fmt_verde)

    # --- 1. PINTAR TABLA DETALLE (Izquierda) ---
        for r in range(1, len(df_p) + 1):
            # Elegimos formato según si la fila es par o impar
            # r % 2 == 0 pintará las filas 2, 4, 6 de Excel (posiciones pares del loop)
            fmt_fila = fmt_azul_pastel if r % 2 == 0 else fmt_blanco
            
            for c in range(len(df_p.columns)):
                val = df_p.iloc[r-1, c]
                worksheet.write(r, c, val, fmt_fila)

        for r in range(1, len(df_tablero)):

            fmt_fila = fmt_amarillo_pastel if r % 2 == 0 else fmt_blanco
            for c in range(len(df_tablero.columns) + 1):
                worksheet.write(r, col_inicio_tablero + c, df_tablero.iloc[r-1, c-1] if c > 0 else df_tablero.index[r-1], fmt_fila)

        # Pintar fila TOTAL en gris
        fila_total = len(df_tablero)
        worksheet.write(fila_total, col_inicio_tablero, "TOTAL", fmt_gris_total)
        for c in range(1, len(df_tablero.columns) + 1):
            worksheet.write(fila_total, col_inicio_tablero + c, df_tablero.iloc[-1, c-1], fmt_gris_total)

        # FORMATO CONDICIONAL: Pintar de ROJO los negativos en TODA la hoja
        worksheet.conditional_format('A1:XFD1048576', {
            'type':     'cell',
            'criteria': '<',
            'value':    0,
            'format':   fmt_negativo
        })

        # Ajustar anchos
        worksheet.set_column(0, 50, 18, fmt_moneda)
    return nombre_archivo

print(f"\n✅ Reporte completo. Los negativos ahora resaltan en rojo y el tablero es amarillo pastel.")

@task

def enviar_mail(path_adjunto: str = None, faltantes: List[str] = None):
    logger = LOGGER_GLOBAL.obtener_logger_prefect()
    msg = MIMEMultipart()
    msg["From"] = MAIL_USER
    msg["To"] = ", ".join(DESTINATARIOS)
    
    fecha_str = datetime.now().strftime("%d de abril") # Ojo: esto es manual según tu pedido

    if path_adjunto:
        msg["Subject"] = "Reporte Pago a Proveedores"
        body = f"Reporte Pago a Proveedores semana del {fecha_str}."
        msg.attach(MIMEText(body, 'plain'))
        with open(path_adjunto, "rb") as f:
            part = MIMEBase("application", "octet-stream")
            part.set_payload(f.read())
            encoders.encode_base64(part)
            part.add_header("Content-Disposition", f"attachment; filename={path_adjunto}")
            msg.attach(part)
    else:
        msg["Subject"] = "Reporte Pago a Proveedores - ATENCION"
        cuerpo_error = "No se envía la planilla por falta de fuentes actualizadas.\n\nFaltan actualizar:\n"
        cuerpo_error += "\n".join([f"- {arc}" for arc in faltantes])
        msg.attach(MIMEText(cuerpo_error, 'plain'))

    server = smtplib.SMTP(MAIL_SERVER, MAIL_PORT)
    server.starttls()
    server.login(MAIL_USER, MAIL_SERVER_PASSWORD)
    server.sendmail(MAIL_USER, DESTINATARIOS, msg.as_string())
    server.quit()
    logger.info("📧 Mail enviado correctamente.")

# --- FLOW ---

@flow(name="Reporte Pago Proveedores Favicur")
def main_flow():
    logger = LOGGER_GLOBAL.obtener_logger_prefect()
    
    fuentes = [
        'Vencimientos de proveedores.xlsx', 
        'Proveedores a pagar - Odoo.xlsx', 
        'eCheqs.xlsx', 
        'Cheques Físicos CBA.xlsx', 
        'Cheques Físicos BS AS.xlsx', 
        'Cuentas Contables.csv', 
        'Cuentas Contables (1).csv'
    ]
    
    # 1. Validar actualización de archivos
 
    
    ok, faltantes = verificar_fuentes_actualizadas(fuentes)
    
    if not ok:
        logger.warning(f"Fuentes desactualizadas: {faltantes}")
        enviar_mail(faltantes=faltantes)
        return
  
    # 2. Si todo está ok, procesar y enviar
    try:
        ruta_excel = generar_reporte_excel()
        enviar_mail(path_adjunto=ruta_excel)
        logger.info("Proceso terminado exitosamente.")
    except Exception as e:
        logger.error(f"Fallo en el procesamiento: {e}")

if __name__ == "__main__":
    main_flow()