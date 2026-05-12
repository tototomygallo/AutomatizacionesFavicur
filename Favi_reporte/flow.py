import pandas as pd
import numpy as np
from datetime import datetime
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
from typing import List, Tuple, Dict
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
#DESTINATARIOS = ["tomas.gallo@consulters.com.ar", "priscila.scharf@consulters.com.ar"]
DESTINATARIOS = ["gmacho@favicur.com.ar", "daguero@favicur.com.ar", "tomas.gallo@consulters.com.ar", "priscila.scharf@consulters.com.ar"]

# CONFIG MAIL (Desde Prefect Secrets)
MAIL_SERVER = "smtp.gmail.com"
MAIL_PORT = 587
MAIL_USER = "tomas.gallo@consulters.com.ar"
MAIL_SERVER_PASSWORD=read_secret("claveemail")

def limpiar_monto_contable(serie):
    return pd.to_numeric(serie.astype(str).str.replace('.', '', regex=False).str.replace(',', '.', regex=False), errors='coerce').fillna(0)

def limpiar_monto_apex(serie):
    return pd.to_numeric(serie, errors='coerce').fillna(0)

def limpiar_monto_valores(serie):
    return pd.to_numeric(serie.astype(str).str.replace(',', '', regex=False), errors='coerce').fillna(0)
# --- AJUSTE EN LA CARGA DE VALORES POR FECHAS ---

def procesar_valores_por_fecha(df, columna_fecha='Fecha Vto.', columna_importe='Importe'):
    df[columna_fecha] = pd.to_datetime(df[columna_fecha], errors='coerce')
    df[columna_importe] = limpiar_monto_valores(df[columna_importe])
    
    
    # Hoy real (sacamos el harcodeo)
    hoy = pd.Timestamp.now().normalize()
    
    # Definimos los límites de ESTA SEMANA
    lunes_esta_semana = hoy - pd.Timedelta(days=hoy.dayofweek) # 04/05
    miercoles = lunes_esta_semana + pd.Timedelta(days=2)       # 06/05
    viernes = lunes_esta_semana + pd.Timedelta(days=4)         # 08/05
    lunes_proximo = lunes_esta_semana + pd.Timedelta(days=7)   # 11/05

    # --- REPARTO SIN MEZCLAR SEMANAS ---
    
    # 1. Miércoles: Solo lo que venció entre el lunes 04 y el miércoles 06 de ESTA semana
    vto_miercoles = df[(df[columna_fecha] >= lunes_esta_semana) & (df[columna_fecha] <= miercoles)][columna_importe].sum()
    #vto_miercoles = df[(df[columna_fecha] <= miercoles)][columna_importe].sum()
  
    # 2. Viernes: Solo lo que vence jueves 07 y viernes 08 de ESTA semana
    vto_viernes = df[(df[columna_fecha] > miercoles) & (df[columna_fecha] <= viernes)][columna_importe].sum()
    
    # 3. Lunes (FCI): Todo lo que sea del lunes que viene (11/05) en adelante
    vto_lunes = df[(df[columna_fecha] > viernes) & (df[columna_fecha] <= lunes_proximo)][columna_importe].sum()
    

    print("MIERCOLES")
    print(df[(df[columna_fecha] >= lunes_esta_semana) & (df[columna_fecha] <= miercoles)][[columna_fecha, columna_importe]])

    print("VIERNES")
    print(df[(df[columna_fecha] > miercoles) & (df[columna_fecha] <= viernes)][[columna_fecha, columna_importe]])

    print("LUNES")
    print(df[(df[columna_fecha] > viernes) & (df[columna_fecha] <= lunes_proximo)][[columna_fecha, columna_importe]])

    return vto_miercoles, vto_viernes, vto_lunes


print("🚀 Iniciando Proceso Integral de Pagos y Disponibilidad...")


@task(retries=2)
def verificar_estado_fuentes(archivos: List[str]) -> Dict[str, str]:
    """Chequea cada archivo y devuelve su estado (Actualizado/Desactualizado)."""
    hoy = datetime.now().date()
    estados = {}
    for arc in archivos:
        path = os.path.join(RUTA_DRIVE, arc)
        if not os.path.exists(path):
            estados[arc] = "❌ No existe en la ruta"
            continue
        
        mtime = datetime.fromtimestamp(os.path.getmtime(path)).date()
        if mtime == hoy:
            estados[arc] = "✅ Actualizado"
        else:
            estados[arc] = f"⚠️ Desactualizado (Últ. cambio: {mtime.strftime('%d/%m')})"
    return estados


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
    # Métodos que pueden pasar a transferencia
    metodos_convertibles = ['Echeq de terceros', 'Cheques CBA', 'Cheques BA']

    # Filtramos solo esos métodos
    mask_convertibles = df_p['Método de pago'].isin(metodos_convertibles)

    # Calculamos el total por proveedor
    # Reemplazá 'Proveedor' por el nombre real de la columna si se llama distinto
    totales_por_proveedor = (
        df_p[mask_convertibles]
        .groupby('Proveedor')['Saldo']
        .sum()
    )

    # Proveedores cuyo total es menor al límite
    proveedores_a_transferencia = totales_por_proveedor[
        totales_por_proveedor < LIMITE_TRANSFERENCIA
    ].index

    # Cambiamos a Transferencia todas las filas de esos proveedores
    df_p.loc[
        mask_convertibles &
        df_p['Proveedor'].isin(proveedores_a_transferencia),
        'Método de pago'
    ] = 'Transferencia'

    # Transferencia -> Galicia
    df_p.loc[
        df_p['Método de pago'] == 'Transferencia',
        'Método de pago'
    ] = 'Galicia'

 # --- 2. CARGAR Y CLASIFICAR VALORES (Desde un solo archivo) ---
    path_valores = os.path.join(RUTA_DRIVE, 'Valores Disponibles.xlsx')
    df_valores_all = pd.read_excel(path_valores)
    #path_valores = pd.read_excel("/home/favidw/favicur/automatizaciones/Python/Valores Disponibles.xlsx")
    
    # Aseguramos que los tipos sean numéricos para filtrar bien
    df_valores_all['Caja'] = pd.to_numeric(df_valores_all['Caja'], errors='coerce')
    df_valores_all['Cod.Tipo'] = pd.to_numeric(df_valores_all['Cod.Tipo'], errors='coerce')

    # Aplicamos los filtros de Pri:
    # 1. Echeqs: Códigos 60 y 61
    df_echeqs = df_valores_all[df_valores_all['Cod.Tipo'].isin([60, 61])].copy()
    
    # 2. Cheques CBA: Códigos 20 o 33 Y Caja 1
    df_cba = df_valores_all[
        (df_valores_all['Cod.Tipo'].isin([20, 33])) & 
        (df_valores_all['Caja'] == 1)
    ].copy()
    
    # 3. Cheques BA: Códigos 20 o 33 Y Caja 5
    df_bsas = df_valores_all[
        (df_valores_all['Cod.Tipo'].isin([20, 33])) & 
        (df_valores_all['Caja'] == 5)
    ].copy()
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
        'Credicoop': 'CREDICOOP', 'Comafi': 'COMAFI', 'Mercado Pago': 'MERCADO PAGO'
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
    # k = Miércoles (Columna K), m = Viernes (Columna M), n = Lunes (Columna N)
    e_k, e_m, e_n = procesar_valores_por_fecha(df_echeqs)
    cba_k, cba_m, cba_n = procesar_valores_por_fecha(df_cba)
    bsas_k, bsas_m, bsas_n = procesar_valores_por_fecha(df_bsas)

    # 4. CONSTRUIR TABLERO (Mapeo exacto)
    # Fila Echeq de terceros
    df_tablero.at['Echeq de terceros', 'saldo online'] = e_k               # Miércoles -> Col K
    df_tablero.at['Echeq de terceros', 'pendientes de acreditacion'] = e_m # Viernes   -> Col M
    df_tablero.at['Echeq de terceros', 'FCI'] = e_n                        # Lunes     -> Col N

    # Fila Cheques CBA
    df_tablero.at['Cheques CBA', 'saldo online'] = cba_k
    df_tablero.at['Cheques CBA', 'pendientes de acreditacion'] = cba_m
    df_tablero.at['Cheques CBA', 'FCI'] = cba_n

    # Fila Cheques BA
    df_tablero.at['Cheques BA', 'saldo online'] = bsas_k
    df_tablero.at['Cheques BA', 'pendientes de acreditacion'] = bsas_m
    df_tablero.at['Cheques BA', 'FCI'] = bsas_n



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

    # --- 5. EXPORTACIÓN CON DISEÑO AVANZADO (CORREGIDO) ---
    df_p = df_p.fillna('')
    nombre_archivo = f'Reporte_Pagos_{datetime.now().strftime("%Y%m%d")}.xlsx'
    
    # SEPARAMOS LAS FILAS DEL TABLERO EN DOS PARA EL EXCEL
    filas_valores = ['Echeq de terceros', 'Cheques CBA', 'Cheques BA']
    df_tablero_bancos = df_tablero.drop(index=filas_valores, errors='ignore')
    # Borramos el total viejo que tenía todo sumado y hacemos uno nuevo solo con bancos
    if 'TOTAL' in df_tablero_bancos.index:
        df_tablero_bancos = df_tablero_bancos.drop(index='TOTAL')
        
    df_tablero_bancos.loc['TOTAL'] = df_tablero_bancos.sum()
    df_tablero_cheques = df_tablero.loc[df_tablero.index.isin(filas_valores)].copy()
    df_tablero_cheques.loc['TOTAL CHEQUES'] = df_tablero_cheques.sum() # <--- Nueva fila de total
    # CAMBIAMOS NOMBRES DE COLUMNAS SOLO PARA LA TABLA DE CHEQUES
    df_tablero_cheques.columns = [
        'proveedores pendientes', 
        'otros pagos pendientes', 
        'miércoles', 
        'saldo-pagos', 
        'viernes', 
        'lunes', 
        'saldo con pendientes y FCI'
    ]

    with pd.ExcelWriter(nombre_archivo, engine='xlsxwriter') as writer:
        # 1. Detalle_Pagos (Tabla Izquierda)
        df_p.to_excel(writer, sheet_name='Tablero Final', index=False, startcol=0)
        
        # 2. Tablero de BANCOS (Tabla Derecha Arriba)
        col_inicio_tablero = len(df_p.columns) + 1
        df_tablero_bancos.to_excel(writer, sheet_name='Tablero Final', startcol=col_inicio_tablero, startrow=0)

        # 3. Tablero de CHEQUES (Tabla Derecha Abajo - 2 renglones de espacio)
        # Calculamos: filas de bancos + header (1) + espacio (2)
        fila_inicio_cheques = len(df_tablero_bancos) + 3
        df_tablero_cheques.to_excel(writer, sheet_name='Tablero Final', startcol=col_inicio_tablero, startrow=fila_inicio_cheques)

        workbook  = writer.book
        worksheet = writer.sheets['Tablero Final']

        # --- TUS FORMATOS (Sin cambios) ---
        fmt_azul = workbook.add_format({'bg_color': '#1F4E78', 'font_color': 'white', 'bold': True, 'border': 1})
        fmt_verde = workbook.add_format({'bg_color': "#2DAC62", 'font_color': 'white', 'bold': True, 'border': 1})
        fmt_blanco = workbook.add_format({'bg_color': "#FFFFFF", 'border': 1, 'num_format': '#,##0.00'})
        fmt_amarillo_pastel = workbook.add_format({'bg_color': "#FFEEBB", 'border': 1, 'num_format': '#,##0.00'})
        fmt_azul_pastel = workbook.add_format({'bg_color': "#BDD4E7", 'border': 1, 'num_format': '#,##0.00'})
        fmt_gris_total = workbook.add_format({'bg_color': '#D9D9D9', 'bold': True, 'border': 1, 'num_format': '#,##0.00'})
        fmt_negativo = workbook.add_format({'font_color': '#9C0006', 'bg_color': '#FFC7CE'})
        fmt_moneda = workbook.add_format({'num_format': '#,##0.00', 'border': 1})
        fmt_rojo = workbook.add_format({'bg_color': "#C04A4A", 'font_color': 'white', 'bold': True, 'border': 1})

        # PINTAR ENCABEZADOS (Detalle y Bancos)
        for col_num, value in enumerate(df_p.columns.values):
            worksheet.write(0, col_num, value, fmt_azul)
        
        worksheet.write(0, col_inicio_tablero, "Concepto", fmt_verde)
        for col_num, value in enumerate(df_tablero_bancos.columns.values):
            worksheet.write(0, col_inicio_tablero + col_num + 1, value, fmt_verde)

        # PINTAR ENCABEZADOS (Cheques - Abajo)
        worksheet.write(fila_inicio_cheques, col_inicio_tablero, "Concepto", fmt_rojo)
        for col_num, value in enumerate(df_tablero_cheques.columns.values):
            worksheet.write(fila_inicio_cheques, col_inicio_tablero + col_num + 1, value, fmt_rojo)

        # PINTAR FILAS DE BANCOS
        for r in range(1, len(df_tablero_bancos) + 1):
            fmt_fila = fmt_amarillo_pastel if r % 2 == 0 else fmt_blanco
            # Si es la última fila (TOTAL), usar gris
            if df_tablero_bancos.index[r-1] == 'TOTAL': fmt_fila = fmt_gris_total
            
            worksheet.write(r, col_inicio_tablero, df_tablero_bancos.index[r-1], fmt_fila)
            for c in range(len(df_tablero_bancos.columns)):
                worksheet.write(r, col_inicio_tablero + c + 1, df_tablero_bancos.iloc[r-1, c], fmt_fila)

        # PINTAR FILAS DE CHEQUES
        for r in range(1, len(df_tablero_cheques) + 1):
            fila_excel = fila_inicio_cheques + r
            fmt_fila = fmt_amarillo_pastel if r % 2 == 0 else fmt_blanco
            if df_tablero_cheques.index[r-1] == 'TOTAL CHEQUES': fmt_fila = fmt_gris_total
            worksheet.write(fila_excel, col_inicio_tablero, df_tablero_cheques.index[r-1], fmt_fila)
            for c in range(len(df_tablero_cheques.columns)):
                worksheet.write(fila_excel, col_inicio_tablero + c + 1, df_tablero_cheques.iloc[r-1, c], fmt_fila)

        # FORMATO CONDICIONAL Y ANCHOS
        worksheet.conditional_format('A1:XFD1048576', {'type': 'cell', 'criteria': '<', 'value': 0, 'format': fmt_negativo})
        worksheet.set_column(0, 50, 18, fmt_moneda)

    return nombre_archivo

print(f"\n✅ Reporte completo. Los negativos ahora resaltan en rojo y el tablero es amarillo pastel.")

@task
def enviar_mail(path_adjunto: str, estados: Dict[str, str]):
    logger = LOGGER_GLOBAL.obtener_logger_prefect()
    msg = MIMEMultipart()
    msg["From"] = MAIL_USER
    msg["To"] = ", ".join(DESTINATARIOS)
    msg["Subject"] = "Reporte Pago a Proveedores - FAVICUR"

    # Construir el cuerpo del mail con el aviso de archivos
    hay_desactualizados = any("✅" not in v for v in estados.values())
    alerta = "⚠️ ATENCIÓN: El reporte contiene datos de archivos desactualizados.\n\n" if hay_desactualizados else ""
    
    cuerpo = f"Hola,\n\nAdjuntamos el Reporte de Pago a Proveedores.\n\n{alerta}Estado de las fuentes utilizadas:\n"
    for archivo, estado in estados.items():
        cuerpo += f"- {archivo}: {estado}\n"
    
    msg.attach(MIMEText(cuerpo, 'plain'))

    with open(path_adjunto, "rb") as f:
        part = MIMEBase("application", "octet-stream")
        part.set_payload(f.read())
        encoders.encode_base64(part)
        part.add_header("Content-Disposition", f"attachment; filename={path_adjunto}")
        msg.attach(part)

    server = smtplib.SMTP(MAIL_SERVER, MAIL_PORT)
    server.starttls()
    server.login(MAIL_USER, MAIL_SERVER_PASSWORD)
    server.sendmail(MAIL_USER, DESTINATARIOS, msg.as_string())
    server.quit()
    logger.info("📧 Mail enviado con éxito.")

# --- FLOW ---

@flow(name="Reporte Pago Proveedores Favicur")
def main_flow():
    logger = LOGGER_GLOBAL.obtener_logger_prefect()
    
    fuentes = [
        'Vencimientos de proveedores.xlsx', 
        'Proveedores a pagar - Odoo.xlsx', 
        #'eCheqs.xlsx', 
        #'Cheques Físicos CBA.xlsx', 
        #'Cheques Físicos BS AS.xlsx',
        'Valores Disponibles.xlsx', 
        'Cuentas Contables.csv', 
        'Cuentas Contables (1).csv'
    ]
    
    # 1. Validar actualización de archivos
 
    
    estados = verificar_estado_fuentes(fuentes)
  
    # 2. Si todo está ok, procesar y enviar
    try:
        ruta_excel = generar_reporte_excel()
        enviar_mail(ruta_excel, estados)
        logger.info("Proceso terminado exitosamente.")
    except Exception as e:
        logger.error(f"Fallo en el procesamiento: {e}")

if __name__ == "__main__":
    main_flow()