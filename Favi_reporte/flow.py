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
import os 

# --- CONFIGURACIÓN DE RUTAS ---
RUTA_DRIVE = '/home/favidw/gdrive/Pago a Proveedores'
LIMITE_TRANSFERENCIA = 300000

# 👇 NOMBRE DE LA COLUMNA DE FECHA EN LA TABLA DE PROVEEDORES
COLUMNA_FECHA_PROVEEDORES = 'Fec.vto' 

# 👇 NOMBRE DEL ARCHIVO MAESTRO QUE GABRIEL SUBE A DRIVE
ARCHIVO_MAESTRO_PAGOS = 'Cruce_intermedio_Pagos.xlsx'

# --- CONFIGURACIÓN ---
LOGGER_GLOBAL = PrefectLogger(__file__)
DESTINATARIOS = ["gmacho@favicur.com.ar", "daguero@favicur.com.ar", "tomas.gallo@consulters.com.ar", "priscila.scharf@consulters.com.ar", "jpinones@favicur.com.ar", "ignacio@favicur.com.ar"]

# CONFIG MAIL (Desde Prefect Secrets)
MAIL_SERVER = "smtp.gmail.com"
MAIL_PORT = 587
MAIL_USER = "tomas.gallo@consulters.com.ar"
MAIL_SERVER_PASSWORD = read_secret("claveemail")

def limpiar_monto_contable(serie):
    return pd.to_numeric(serie.astype(str).str.replace('.', '', regex=False).str.replace(',', '.', regex=False), errors='coerce').fillna(0)

def limpiar_monto_apex(serie):
    return pd.to_numeric(serie, errors='coerce').fillna(0)

def limpiar_monto_valores(serie):
    return pd.to_numeric(serie.astype(str).str.replace(',', '', regex=False), errors='coerce').fillna(0)

def limpiar_monto_impuestos_seguro(celda):
    if pd.isna(celda):
        return 0.0
    if isinstance(celda, (int, float)):
        return float(celda)
    s = str(celda).strip()
    if not s or s == '-' or '$ -' in s:
        return 0.0
    s = s.split('(')[0].strip()
    s = s.replace('$', '').strip()
    if ',' in s and '.' in s:
        s = s.replace('.', '').replace(',', '.')
    elif ',' in s and '.' not in s:
        s = s.replace(',', '.')
    try:
        return float(s)
    except ValueError:
        return 0.0

def procesar_valores_por_fecha(df, hoy_base, columna_fecha='Fecha Vto.', columna_importe='Importe'):
    df[columna_fecha] = pd.to_datetime(df[columna_fecha], errors='coerce')
    df[columna_importe] = limpiar_monto_valores(df[columna_importe])
    
    lunes_esta_semana = hoy_base - pd.Timedelta(days=hoy_base.dayofweek) 
    miercoles = lunes_esta_semana + pd.Timedelta(days=2)       
    viernes = lunes_esta_semana + pd.Timedelta(days=4)         
    lunes_proximo = lunes_esta_semana + pd.Timedelta(days=7)   
    inicio_ventana_miercoles = miercoles - pd.Timedelta(days=28)
    
    vto_miercoles = df[(df[columna_fecha] >= inicio_ventana_miercoles) & (df[columna_fecha] <= miercoles)][columna_importe].sum()
    vto_viernes = df[(df[columna_fecha] > miercoles) & (df[columna_fecha] <= viernes)][columna_importe].sum()
    vto_lunes = df[(df[columna_fecha] > viernes) & (df[columna_fecha] <= lunes_proximo)][columna_importe].sum()
    
    return vto_miercoles, vto_viernes, vto_lunes


@task(retries=2)
def verificar_estado_fuentes(archivos: List[str]) -> Dict[str, str]:
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


def calcular_datos_tablero(hoy_base):
    logger = LOGGER_GLOBAL.obtener_logger_prefect()
    
    lunes_semana = hoy_base - pd.Timedelta(days=hoy_base.dayofweek)
    domingo_semana = lunes_semana + pd.Timedelta(days=6)

    # -------------------------------------------------------------------------
    # 1. CARGAR DATOS DE PAGOS Y FILTRAR POR SEMANA
    # -------------------------------------------------------------------------
    path_maestro = os.path.join(RUTA_DRIVE, ARCHIVO_MAESTRO_PAGOS)
    df_p = pd.read_excel(path_maestro)

    mapeo_columnas = {}
    for col in df_p.columns:
        col_str = str(col)
        if 'NAMERO' in col_str.upper() or 'NÃ' in col_str or 'NÚMERO' in col_str.upper() or 'NUMERO' in col_str.upper():
            mapeo_columnas[col] = 'Número'
    if mapeo_columnas:
        df_p = df_p.rename(columns=mapeo_columnas)

    df_p[COLUMNA_FECHA_PROVEEDORES] = pd.to_datetime(df_p[COLUMNA_FECHA_PROVEEDORES], errors='coerce')
    df_p = df_p[(df_p[COLUMNA_FECHA_PROVEEDORES] >= lunes_semana) & (df_p[COLUMNA_FECHA_PROVEEDORES] <= domingo_semana)].copy()
    
    logger.info(f"Para el rango {lunes_semana.strftime('%d/%m')} al {domingo_semana.strftime('%d/%m')}, se encontraron {len(df_p)} registros de proveedores.")

    df_p['Saldo'] = limpiar_monto_apex(df_p['Saldo'])
    
    # 👇 CAMBIO: Pasamos temporalmente todo a MAYÚSCULAS para estandarizar errores de tipeo
    df_p['Método de pago'] = df_p['Método de pago'].fillna('#N/D').astype(str).str.strip().str.upper()

    # CAMBIO: Lógica de convertibles adaptada a las MAYÚSCULAS
    metodos_convertibles = ['ECHEQ DE TERCEROS', 'CHEQUES CBA', 'CHEQUES BA']
    mask_convertibles = df_p['Método de pago'].isin(metodos_convertibles)

    totales_por_proveedor = df_p[mask_convertibles].groupby('Proveedor')['Saldo'].sum()
    proveedores_a_transferencia = totales_por_proveedor[totales_por_proveedor < LIMITE_TRANSFERENCIA].index

    df_p.loc[mask_convertibles & df_p['Proveedor'].isin(proveedores_a_transferencia), 'Método de pago'] = 'TRANSFERENCIA'
    df_p.loc[df_p['Método de pago'] == 'TRANSFERENCIA', 'Método de pago'] = 'GALICIA'
    
    # 👇 CAMBIO: Mapeo estricto para traducir las mayúsculas al nombre exacto del Tablero
    mapeo_metodos_tablero = {
        'MERCADO PAGO': 'Mercado Pago',
        'MERCADOPAGO': 'Mercado Pago',
        'BANCOR': 'Bancor',
        'NACION': 'Nación',
        'NACIÓN': 'Nación',
        'ICBC': 'ICBC',
        'PATAGONIA': 'Patagonia',
        'MACRO': 'Macro',
        'CREDICOOP': 'Credicoop',
        'COMAFI': 'Comafi',
        'GALICIA': 'Galicia',
        'BECERRA/BALANZ': 'Becerra/Balanz',
        'BALANZ': 'Becerra/Balanz',
        'TSA': 'TSA',
        'EFECTIVO': 'Efectivo',
        'ECHEQ DE TERCEROS': 'Echeq de terceros',
        'CHEQUES CBA': 'Cheques CBA',
        'CHEQUES BA': 'Cheques BA'
    }
    
    # Reemplazamos los valores por el nombre correcto del tablero
    df_p['Método de pago'] = df_p['Método de pago'].map(mapeo_metodos_tablero).fillna(df_p['Método de pago'].str.title())
    
    # 👇 AGREGAR FILA DE TOTAL PARA EL CRUCE DE PROVEEDORES (IZQUIERDA)
    if not df_p.empty:
        total_saldo_cruce = df_p['Saldo'].sum()
        fila_total_p = {col: '' for col in df_p.columns}
        
        if 'Proveedor' in fila_total_p:
            fila_total_p['Proveedor'] = 'TOTAL GENERAL'
        else:
            fila_total_p[df_p.columns[0]] = 'TOTAL GENERAL'
            
        if 'Saldo' in fila_total_p:
            fila_total_p['Saldo'] = total_saldo_cruce
            
        df_p = pd.concat([df_p, pd.DataFrame([fila_total_p])], ignore_index=True)
    # -------------------------------------------------------------------------

    # 2. CARGAR Y CLASIFICAR VALORES
    path_valores = os.path.join(RUTA_DRIVE, 'Valores Disponibles.xlsx')
    df_valores_all = pd.read_excel(path_valores)
    df_valores_all['Caja'] = pd.to_numeric(df_valores_all['Caja'], errors='coerce')
    df_valores_all['Cod.Tipo'] = pd.to_numeric(df_valores_all['Cod.Tipo'], errors='coerce')

    df_echeqs = df_valores_all[df_valores_all['Cod.Tipo'].isin([60, 61])].copy()
    df_cba = df_valores_all[(df_valores_all['Cod.Tipo'].isin([20, 33])) & (df_valores_all['Caja'] == 1)].copy()
    df_bsas = df_valores_all[(df_valores_all['Cod.Tipo'].isin([20, 33])) & (df_valores_all['Caja'] == 5)].copy()

    # --- PROCESAR VALORES IMPOSITIVOS ---
    path_valores_imp = os.path.join(RUTA_DRIVE, 'Vencimientos Impositivos Favicur.xlsx')
    df_imp_raw = pd.read_excel(path_valores_imp, header=3)
    
    if "A.F.I.F." in str(df_imp_raw.columns[0]) or df_imp_raw.iloc[0].astype(str).str.contains('CONCEPTO').any():
        for idx, row in df_imp_raw.iterrows():
            if row.astype(str).str.contains('CONCEPTO').any():
                df_imp_raw = pd.read_excel(path_valores_imp, skiprows=idx + 1)
                break

    col_concepto = [c for c in df_imp_raw.columns if 'CONCEPTO' in str(c).upper()][0]
    col_fecha_vto = [c for c in df_imp_raw.columns if 'FECHA VTO' in str(c).upper()][0]
    columnas_2026 = [c for c in df_imp_raw.columns if '2026' in str(c)]
    
    if columnas_2026:
        col_monto_objetivo = columnas_2026[0]
    else:
        col_monto_objetivo = df_imp_raw.columns[4]

    df_imp_temp = df_imp_raw[[col_concepto, col_fecha_vto, col_monto_objetivo]].copy()
    df_imp_temp.columns = ['Concepto', 'Fecha Vto. Original', 'Monto Original']
    
    df_imp_temp['Monto Original'] = df_imp_temp['Monto Original'].apply(limpiar_monto_impuestos_seguro)
    df_imp_temp['Fecha Vto. Datetime'] = pd.to_datetime(df_imp_temp['Fecha Vto. Original'], errors='coerce')
    df_imp_temp = df_imp_temp[(df_imp_temp['Concepto'].notna()) & (df_imp_temp['Monto Original'] > 0)]

    # --- CALCULO DIARIO BASADO EN EL HOY DINÁMICO ---
    martes_esta_semana = lunes_semana + pd.Timedelta(days=1)
    miercoles_esta_semana = lunes_semana + pd.Timedelta(days=2)
    jueves_esta_semana = lunes_semana + pd.Timedelta(days=3)
    viernes_esta_semana = lunes_semana + pd.Timedelta(days=4)

    df_otros_pagos = pd.DataFrame()
    df_otros_pagos['Concepto'] = df_imp_temp['Concepto']
    df_otros_pagos['Banco'] = 'MACRO'

    df_otros_pagos['Lunes'] = np.where(df_imp_temp['Fecha Vto. Datetime'] == lunes_semana, df_imp_temp['Monto Original'], 0.0)
    df_otros_pagos['Martes'] = np.where(df_imp_temp['Fecha Vto. Datetime'] == martes_esta_semana, df_imp_temp['Monto Original'], 0.0)
    df_otros_pagos['Miércoles'] = np.where(df_imp_temp['Fecha Vto. Datetime'] == miercoles_esta_semana, df_imp_temp['Monto Original'], 0.0)
    df_otros_pagos['Jueves'] = np.where(df_imp_temp['Fecha Vto. Datetime'] == jueves_esta_semana, df_imp_temp['Monto Original'], 0.0)
    df_otros_pagos['Viernes'] = np.where(df_imp_temp['Fecha Vto. Datetime'] == viernes_esta_semana, df_imp_temp['Monto Original'], 0.0)

    df_otros_pagos['Semana'] = (
        df_otros_pagos['Lunes'] + 
        df_otros_pagos['Martes'] + 
        df_otros_pagos['Miércoles'] + 
        df_otros_pagos['Jueves'] + 
        df_otros_pagos['Viernes']
    )
    
    columnas_ordenadas = ['Concepto', 'Banco', 'Semana', 'Lunes', 'Martes', 'Miércoles', 'Jueves', 'Viernes']
    df_otros_pagos = df_otros_pagos[columnas_ordenadas]
    df_otros_pagos = df_otros_pagos[df_otros_pagos['Semana'] > 0].copy()

    # 👇 SE AGREGA FILA DE TOTAL PARA LA TABLA DE OTROS PAGOS (IMPUESTOS)
    if not df_otros_pagos.empty:
        totales_imp = df_otros_pagos[['Semana', 'Lunes', 'Martes', 'Miércoles', 'Jueves', 'Viernes']].sum()
        fila_total_imp = pd.DataFrame([{
            'Concepto': 'TOTAL IMPUESTOS',
            'Banco': '',
            'Semana': totales_imp['Semana'],
            'Lunes': totales_imp['Lunes'],
            'Martes': totales_imp['Martes'],
            'Miércoles': totales_imp['Miércoles'],
            'Jueves': totales_imp['Jueves'],
            'Viernes': totales_imp['Viernes']
        }])
        df_otros_pagos = pd.concat([df_otros_pagos, fila_total_imp], ignore_index=True)

    # 3. CARGAR SALDOS CONTABLES Y FCI
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

    mapa_bancos = {
        'Bancor': 'PCIA DE CORDOBA', 'Nación': 'NACION', 'ICBC': 'ICBC', 
        'Patagonia': 'PATAGONIA', 'Macro': 'MACRO', 'Galicia': 'GALICIA', 
        'Credicoop': 'CREDICOOP', 'Comafi': 'COMAFI', 'Mercado Pago': 'MERCADO PAGO'
    }
    for b, term in mapa_bancos.items():
        saldo = df_cc[df_cc['Descripcion'].str.contains(term, na=False)]['Saldo fecha'].sum()
        df_tablero.at[b, 'saldo online'] = saldo

    # Agrupar pagos (excluyendo la fila de TOTAL GENERAL para no duplicar en el tablero)
    resumen_pagos = df_p[df_p['Proveedor'] != 'TOTAL GENERAL'].groupby('Método de pago')['Saldo'].sum()
    for metodo, monto in resumen_pagos.items():
        if metodo in df_tablero.index:
            df_tablero.at[metodo, 'proveedores pendientes'] = monto

    # Asignar también el total de otros pagos al banco MACRO en el tablero unificado
    if not df_otros_pagos.empty:
        total_semana_imp = df_otros_pagos[df_otros_pagos['Concepto'] == 'TOTAL IMPUESTOS']['Semana'].values[0]
        df_tablero.at['Macro', 'otros pagos pendientes'] = total_semana_imp

    mapa_fci = {'Macro': 'MACRO', 'Credicoop': 'CREDICOOP', 'Patagonia': 'PATAGONIA', 'Bancor': 'CORDOBA', 'Galicia': 'GALICIA', 'Comafi': 'COMAFI'}
    for b, term in mapa_fci.items():
        fci_valor = df_fci[df_fci['Descripcion'].str.contains(term, na=False)]['Saldo fecha'].sum()
        df_tablero.at[b, 'FCI'] = fci_valor

    e_k, e_m, e_n = procesar_valores_por_fecha(df_echeqs, hoy_base)
    cba_k, cba_m, cba_n = procesar_valores_por_fecha(df_cba, hoy_base)
    bsas_k, bsas_m, bsas_n = procesar_valores_por_fecha(df_bsas, hoy_base)

    df_tablero.at['Echeq de terceros', 'saldo online'] = e_k                
    df_tablero.at['Echeq de terceros', 'pendientes de acreditacion'] = e_m 
    df_tablero.at['Echeq de terceros', 'FCI'] = e_n                        

    df_tablero.at['Cheques CBA', 'saldo online'] = cba_k
    df_tablero.at['Cheques CBA', 'pendientes de acreditacion'] = cba_m
    df_tablero.at['Cheques CBA', 'FCI'] = cba_n

    df_tablero.at['Cheques BA', 'saldo online'] = bsas_k
    df_tablero.at['Cheques BA', 'pendientes de acreditacion'] = bsas_m
    df_tablero.at['Cheques BA', 'FCI'] = bsas_n

    df_tablero['saldo-pagos'] = df_tablero['saldo online'] - df_tablero['proveedores pendientes'] - df_tablero['otros pagos pendientes']
    df_tablero['saldo con pendientes y FCI'] = (
        df_tablero['saldo-pagos'] + 
        df_tablero['pendientes de acreditacion'] + 
        df_tablero['FCI']
    )
    df_tablero.loc['TOTAL'] = df_tablero.sum()
    df_tablero = df_tablero.rename(columns={'saldo online': 'saldo contable'})

    # Formatear la fecha del Cruce antes de exportar
    if COLUMNA_FECHA_PROVEEDORES in df_p.columns:
        df_p[COLUMNA_FECHA_PROVEEDORES] = pd.to_datetime(df_p[COLUMNA_FECHA_PROVEEDORES], errors='coerce').dt.strftime('%Y-%m-%d')

    df_p = df_p.fillna('')
    
    filas_valores = ['Echeq de terceros', 'Cheques CBA', 'Cheques BA']
    df_tablero_bancos = df_tablero.drop(index=filas_valores, errors='ignore')
    if 'TOTAL' in df_tablero_bancos.index:
        df_tablero_bancos = df_tablero_bancos.drop(index='TOTAL')
        
    df_tablero_bancos.loc['TOTAL'] = df_tablero_bancos.sum()
    df_tablero_cheques = df_tablero.loc[df_tablero.index.isin(filas_valores)].copy()
    df_tablero_cheques.loc['TOTAL CHEQUES'] = df_tablero_cheques.sum() 
    
    df_tablero_cheques.columns = [
        'proveedores pendientes', 
        'otros pagos pendientes', 
        'miércoles', 
        'saldo-pagos', 
        'viernes', 
        'lunes', 
        'saldo con pendientes y FCI'
    ]

    return df_p, df_tablero_bancos, df_tablero_cheques, df_otros_pagos


def escribir_hoja_tablero(writer, nombre_hoja, df_p, df_tablero_bancos, df_tablero_cheques, df_otros_pagos):
    df_p.to_excel(writer, sheet_name=nombre_hoja, index=False, startcol=0)
    
    col_inicio_tablero = len(df_p.columns) + 1
    df_tablero_bancos.to_excel(writer, sheet_name=nombre_hoja, startcol=col_inicio_tablero, startrow=0)

    fila_inicio_cheques = len(df_tablero_bancos) + 3
    df_tablero_cheques.to_excel(writer, sheet_name=nombre_hoja, startcol=col_inicio_tablero, startrow=fila_inicio_cheques)

    fila_inicio_impuestos = fila_inicio_cheques + len(df_tablero_cheques) + 3
    df_otros_pagos.to_excel(writer, sheet_name=nombre_hoja, startcol=col_inicio_tablero, startrow=fila_inicio_impuestos, index=False)

    workbook  = writer.book
    worksheet = writer.sheets[nombre_hoja]

    fmt_azul = workbook.add_format({'bg_color': '#1F4E78', 'font_color': 'white', 'bold': True, 'border': 1})
    fmt_verde = workbook.add_format({'bg_color': "#2DAC62", 'font_color': 'white', 'bold': True, 'border': 1})
    fmt_blanco = workbook.add_format({'bg_color': "#FFFFFF", 'border': 1, 'num_format': '#,##0.00'})
    fmt_amarillo_pastel = workbook.add_format({'bg_color': "#FFEEBB", 'border': 1, 'num_format': '#,##0.00'})
    fmt_gris_total = workbook.add_format({'bg_color': '#D9D9D9', 'bold': True, 'border': 1, 'num_format': '#,##0.00'})
    fmt_negativo = workbook.add_format({'font_color': '#9C0006', 'bg_color': '#FFC7CE'})
    fmt_moneda = workbook.add_format({'num_format': '#,##0.00', 'border': 1})
    fmt_rojo = workbook.add_format({'bg_color': "#C04A4A", 'font_color': 'white', 'bold': True, 'border': 1})
    fmt_gris_oscuro = workbook.add_format({'bg_color': "#595959", 'font_color': 'white', 'bold': True, 'border': 1})

    # 👇 1. Encabezados e inyección de estilos para la tabla Cruce de Proveedores
    for col_num, value in enumerate(df_p.columns.values):
        worksheet.write(0, col_num, value, fmt_azul)
        
    for r in range(1, len(df_p) + 1):
        # Detectamos si es la última fila (la que agregamos de TOTAL GENERAL)
        es_total_cruce = (str(df_p.iloc[r-1].get('Proveedor', '')) == 'TOTAL GENERAL' or 
                          str(df_p.iloc[r-1].iloc[0]) == 'TOTAL GENERAL')
        
        fmt_fila = fmt_gris_total if es_total_cruce else (fmt_amarillo_pastel if r % 2 == 0 else fmt_blanco)
        
        for c in range(len(df_p.columns)):
            val = df_p.iloc[r-1, c]
            # Si es celda numérica, escribir como float para que el formato de moneda aplique
            if isinstance(val, (int, float)) and val != '':
                worksheet.write(r, c, float(val), fmt_fila)
            else:
                worksheet.write(r, c, str(val), fmt_fila)

    # Encabezados Tablero Bancos
    worksheet.write(0, col_inicio_tablero, "Concepto", fmt_verde)
    for col_num, value in enumerate(df_tablero_bancos.columns.values):
        worksheet.write(0, col_inicio_tablero + col_num + 1, value, fmt_verde)

    # Encabezados Tablero Cheques
    worksheet.write(fila_inicio_cheques, col_inicio_tablero, "Concepto", fmt_rojo)
    for col_num, value in enumerate(df_tablero_cheques.columns.values):
        worksheet.write(fila_inicio_cheques, col_inicio_tablero + col_num + 1, value, fmt_rojo)

    # Encabezados Impuestos
    for col_num, value in enumerate(df_otros_pagos.columns.values):
        worksheet.write(fila_inicio_impuestos, col_inicio_tablero + col_num, value, fmt_gris_oscuro)

    # 2. Formateo de Tabla de Bancos
    for r in range(1, len(df_tablero_bancos) + 1):
        fmt_fila = fmt_amarillo_pastel if r % 2 == 0 else fmt_blanco
        if df_tablero_bancos.index[r-1] == 'TOTAL': 
            fmt_fila = fmt_gris_total
        
        worksheet.write(r, col_inicio_tablero, df_tablero_bancos.index[r-1], fmt_fila)
        for c in range(len(df_tablero_bancos.columns)):
            worksheet.write(r, col_inicio_tablero + c + 1, df_tablero_bancos.iloc[r-1, c], fmt_fila)

    # 3. Formateo de Tabla de Cheques
    for r in range(1, len(df_tablero_cheques) + 1):
        fila_excel = fila_inicio_cheques + r
        fmt_fila = fmt_amarillo_pastel if r % 2 == 0 else fmt_blanco
        if df_tablero_cheques.index[r-1] == 'TOTAL CHEQUES': 
            fmt_fila = fmt_gris_total
            
        worksheet.write(fila_excel, col_inicio_tablero, df_tablero_cheques.index[r-1], fmt_fila)
        for c in range(len(df_tablero_cheques.columns)):
            worksheet.write(fila_excel, col_inicio_tablero + c + 1, df_tablero_cheques.iloc[r-1, c], fmt_fila)

    # 4. Formateo de Tabla de Impuestos
    for r in range(1, len(df_otros_pagos) + 1):
        fila_excel = fila_inicio_impuestos + r
        fmt_fila = fmt_amarillo_pastel if r % 2 == 0 else fmt_blanco
        
        if str(df_otros_pagos.iloc[r-1, 0]) == 'TOTAL IMPUESTOS':
            fmt_fila = fmt_gris_total
        
        worksheet.write(fila_excel, col_inicio_tablero, str(df_otros_pagos.iloc[r-1, 0]), fmt_fila)
        worksheet.write(fila_excel, col_inicio_tablero + 1, str(df_otros_pagos.iloc[r-1, 1]), fmt_fila)
        
        for c in range(2, len(df_otros_pagos.columns)):
            worksheet.write(fila_excel, col_inicio_tablero + c, float(df_otros_pagos.iloc[r-1, c]), fmt_fila)

    worksheet.conditional_format('A1:XFD1048576', {'type': 'cell', 'criteria': '<', 'value': 0, 'format': fmt_negativo})
    worksheet.set_column(0, 50, 18, fmt_moneda)


@task
def generar_reporte_excel():
    hoy_actual = pd.Timestamp.now().normalize()
    hoy_entrante = hoy_actual + pd.Timedelta(days=7)

    p_act, ban_act, chq_act, imp_act = calcular_datos_tablero(hoy_actual)
    p_ent, ban_ent, chq_ent, imp_ent = calcular_datos_tablero(hoy_entrante)

    nombre_archivo = f'Reporte_Pagos_{datetime.now().strftime("%Y%m%d")}.xlsx'

    with pd.ExcelWriter(nombre_archivo, engine='xlsxwriter') as writer:
        escribir_hoja_tablero(writer, 'Semana en curso', p_act, ban_act, chq_act, imp_act)
        escribir_hoja_tablero(writer, 'Semana entrante', p_ent, ban_ent, chq_ent, imp_ent)

    return nombre_archivo


@task
def enviar_mail(path_adjunto: str, estados: Dict[str, str]):
    logger = LOGGER_GLOBAL.obtener_logger_prefect()
    msg = MIMEMultipart()
    msg["From"] = MAIL_USER
    msg["To"] = ", ".join(DESTINATARIOS)
    msg["Subject"] = "Reporte Pago a Proveedores - FAVICUR"

    hay_desactualizados = any("✅" not in v for v in estados.values())
    alerta = "⚠️ ATENCIÓN: El reporte contiene datos de archivos desactualizados.\n\n" if hay_desactualizados else ""
    
    cuerpo = f"Hola,\n\nAdjuntamos el Reporte de Pago a Proveedores (incluye pestañas de Semana en curso y Semana entrante).\n\n{alerta}Estado de las fuentes utilizadas:\n"
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
@flow(name="Reporte Pago Proveedores Favicur - Final")
def main_flow():
    logger = LOGGER_GLOBAL.obtener_logger_prefect()
    
    fuentes = [
        ARCHIVO_MAESTRO_PAGOS, 
        'Valores Disponibles.xlsx', 
        'Cuentas Contables.csv', 
        'Cuentas Contables (1).csv',
        'Vencimientos Impositivos Favicur.xlsx'
    ]
    
    estados = verificar_estado_fuentes(fuentes)
  
    try:
        ruta_excel = generar_reporte_excel()
        enviar_mail(ruta_excel, estados)
        logger.info("Proceso terminado exitosamente.")
    except Exception as e:
        logger.error(f"Fallo en el procesamiento: {e}")

if __name__ == "__main__":
    main_flow()