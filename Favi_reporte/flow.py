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
#DESTINATARIOS = ["tomas.gallo@consulters.com.ar"]
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
    martes = lunes_esta_semana + pd.Timedelta(days=1)
    miercoles = lunes_esta_semana + pd.Timedelta(days=2)       
    jueves = lunes_esta_semana + pd.Timedelta(days=3)
    viernes = lunes_esta_semana + pd.Timedelta(days=4)         
    
    inicio_ventana_lunes = lunes_esta_semana - pd.Timedelta(days=28)
    
    vto_lunes = df[(df[columna_fecha] >= inicio_ventana_lunes) & (df[columna_fecha] <= lunes_esta_semana)][columna_importe].sum()
    vto_martes = df[df[columna_fecha] == martes][columna_importe].sum()
    vto_miercoles = df[df[columna_fecha] == miercoles][columna_importe].sum()
    vto_jueves = df[df[columna_fecha] == jueves][columna_importe].sum()
    vto_viernes = df[df[columna_fecha] == viernes][columna_importe].sum()
    
    return vto_lunes, vto_martes, vto_miercoles, vto_jueves, vto_viernes


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


def normalizar_termino_pago(val):
    if pd.isna(val):
        return "Sin Especificar"
    
    s = str(val).strip().lower()
    numeros = ''.join([c for c in s if c.isdigit()])
    
    if numeros:
        if numeros == "0":
            return "Pago inmediato"
        else:
            return f"A {numeros} days"
            
    if 'inmediato' in s:
        return "Pago inmediato"
        
    return val.strip().capitalize()


def calcular_datos_tablero(hoy_base):
    logger = LOGGER_GLOBAL.obtener_logger_prefect()
    
    lunes_semana = hoy_base - pd.Timedelta(days=hoy_base.dayofweek)
    domingo_semana = lunes_semana + pd.Timedelta(days=6)

    # 1. CARGAR DATOS DE PAGOS Y FILTRAR POR SEMANA
    path_maestro = os.path.join(RUTA_DRIVE, ARCHIVO_MAESTRO_PAGOS)
    df_p = pd.read_excel(path_maestro)

    mapeo_columnas = {}
    for col in df_p.columns:
        col_str = str(col)
        if any(x in col_str.upper() for x in ['NAMERO', 'NÃ', 'NÚMERO', 'NUMERO']):
            mapeo_columnas[col] = 'Número'
    if mapeo_columnas:
        df_p = df_p.rename(columns=mapeo_columnas)

    df_p[COLUMNA_FECHA_PROVEEDORES] = pd.to_datetime(df_p[COLUMNA_FECHA_PROVEEDORES], errors='coerce')
    df_p = df_p[(df_p[COLUMNA_FECHA_PROVEEDORES] >= lunes_semana) & (df_p[COLUMNA_FECHA_PROVEEDORES] <= domingo_semana)].copy()
    
    logger.info(f"Para el rango {lunes_semana.strftime('%d/%m')} al {domingo_semana.strftime('%d/%m')}, se encontraron {len(df_p)} registros de proveedores.")

    df_p['Saldo'] = limpiar_monto_apex(df_p['Saldo'])
    df_p['Método de pago'] = df_p['Método de pago'].fillna('#N/D').astype(str).str.strip().str.upper()

    metodos_convertibles = ['ECHEQ DE TERCEROS', 'CHEQUES CBA', 'CHEQUES BA']
    mask_convertibles = df_p['Método de pago'].isin(metodos_convertibles)

    totales_por_proveedor = df_p[mask_convertibles].groupby('Proveedor')['Saldo'].sum()
    proveedores_a_transferencia = totales_por_proveedor[totales_por_proveedor < LIMITE_TRANSFERENCIA].index

    df_p.loc[mask_convertibles & df_p['Proveedor'].isin(proveedores_a_transferencia), 'Método de pago'] = 'TRANSFERENCIA'
    df_p.loc[df_p['Método de pago'] == 'TRANSFERENCIA', 'Método de pago'] = 'GALICIA'
    
    # 2. CARGAR Y CLASIFICAR VALORES (Se adelanta para usar df_echeqs en la expansión)
    path_valores = os.path.join(RUTA_DRIVE, 'Valores Disponibles.xlsx')
    df_valores_all = pd.read_excel(path_valores)
    df_valores_all['Caja'] = pd.to_numeric(df_valores_all['Caja'], errors='coerce')
    df_valores_all['Cod.Tipo'] = pd.to_numeric(df_valores_all['Cod.Tipo'], errors='coerce')

    df_echeqs = df_valores_all[df_valores_all['Cod.Tipo'].isin([60, 61])].copy()
    df_cba = df_valores_all[(df_valores_all['Cod.Tipo'].isin([20, 33])) & (df_valores_all['Caja'] == 1)].copy()
    df_bsas = df_valores_all[(df_valores_all['Cod.Tipo'].isin([20, 33])) & (df_valores_all['Caja'] == 5)].copy()

    # --- CONSTRUCCIÓN TABLA NUEVA: EXPANSIÓN ECHEQS DE TERCEROS ---
    col_terminos = [
        c for c in df_p.columns 
        if 'PAGO' in str(c).upper() and ('MIN' in str(c).upper() or 'TERM' in str(c).upper())
    ]    
    
    modalidades_fijas = [
        "Pago inmediato", 
        "A 7 days", 
        "A 15 days", 
        "A 30 days", 
        "A 45 days", 
        "A 60 days", 
        "A 90 days", 
        "A 120 days"
    ]
    
    # Base inicial limpia para el reporte de expansión
    df_base_exp = pd.DataFrame(index=modalidades_fijas)
    df_base_exp['Monto Pendiente'] = 0.0
    df_base_exp['Monto Disponible'] = 0.0
    df_base_exp.index.name = 'Modalidad Echeqs'

    # Calcular Monto Pendiente (desde Proveedores)
    if col_terminos:
        col_actual_terminos = col_terminos[0]
        df_solo_echeqs = df_p[df_p['Método de pago'] == 'ECHEQ DE TERCEROS'].copy()
        if not df_solo_echeqs.empty:
            df_solo_echeqs['Termino_Normalizado'] = df_solo_echeqs[col_actual_terminos].apply(normalizar_termino_pago)
            pendientes_agrupados = df_solo_echeqs.groupby('Termino_Normalizado')['Saldo'].sum()
            for k, v in pendientes_agrupados.items():
                if k in df_base_exp.index:
                    df_base_exp.at[k, 'Monto Pendiente'] = v

    # Calcular Monto Disponible de forma Real (desde df_echeqs)
    if not df_echeqs.empty:
        df_echeqs['Fecha Vto.'] = pd.to_datetime(df_echeqs['Fecha Vto.'], errors='coerce')
        df_echeqs['Importe'] = limpiar_monto_valores(df_echeqs['Importe'])
        
        # Clasificar según la distancia en días a hoy_base
        dias_vto = (df_echeqs['Fecha Vto.'] - hoy_base).dt.days
        
        condiciones = [
            (dias_vto <= 0),
            (dias_vto > 0) & (dias_vto <= 7),
            (dias_vto > 7) & (dias_vto <= 15),
            (dias_vto > 15) & (dias_vto <= 30),
            (dias_vto > 30) & (dias_vto <= 45),
            (dias_vto > 45) & (dias_vto <= 60),
            (dias_vto > 60) & (dias_vto <= 90),
            (dias_vto > 90)
        ]
        
        df_echeqs['Modalidad_Calculada'] = np.select(condiciones, modalidades_fijas, default="Sin Especificar")
        disponibles_agrupados = df_echeqs.groupby('Modalidad_Calculada')['Importe'].sum()
        for k, v in disponibles_agrupados.items():
            if k in df_base_exp.index:
                df_base_exp.at[k, 'Monto Disponible'] = v

    # Estructura final con totales de expansión
    df_agrupado_echeqs = df_base_exp.reset_index()
    tot_pendiente = df_agrupado_echeqs['Monto Pendiente'].sum()
    tot_disponible = df_agrupado_echeqs['Monto Disponible'].sum()
    
    fila_tot_exp = pd.DataFrame([{
        'Modalidad Echeqs': 'TOTAL EXPANSION', 
        'Monto Pendiente': tot_pendiente,
        'Monto Disponible': tot_disponible
    }])
    df_expansion_echeqs = pd.concat([df_agrupado_echeqs, fila_tot_exp], ignore_index=True)

    mapeo_metodos_tablero = {
        'MERCADO PAGO': 'Mercado Pago', 'MERCADOPAGO': 'Mercado Pago', 'BANCOR': 'Bancor',
        'NACION': 'Nación', 'NACIÓN': 'Nación', 'ICBC': 'ICBC', 'PATAGONIA': 'Patagonia',
        'MACRO': 'Macro', 'CREDICOOP': 'Credicoop', 'COMAFI': 'Comafi', 'GALICIA': 'Galicia',
        'BECERRA/BALANZ': 'Becerra/Balanz', 'BALANZ': 'Becerra/Balanz', 'TSA': 'TSA',
        'EFECTIVO': 'Efectivo', 'ECHEQ DE TERCEROS': 'Echeq de terceros',
        'CHEQUES CBA': 'Cheques CBA', 'CHEQUES BA': 'Cheques BA'
    }
    
    df_p['Método de pago'] = df_p['Método de pago'].map(mapeo_metodos_tablero).fillna(df_p['Método de pago'].str.title())
    
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

    if not df_otros_pagos.empty:
        totales_imp = df_otros_pagos[['Semana', 'Lunes', 'Martes', 'Miércoles', 'Jueves', 'Viernes']].sum()
        fila_total_imp = pd.DataFrame([{
            'Concepto': 'TOTAL IMPUESTOS', 'Banco': '', 'Semana': totales_imp['Semana'],
            'Lunes': totales_imp['Lunes'], 'Martes': totales_imp['Martes'], 'Miércoles': totales_imp['Miércoles'],
            'Jueves': totales_imp['Jueves'], 'Viernes': totales_imp['Viernes']
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
    
    columnas = ['proveedores pendientes', 'otros pagos pendientes', 'saldo online', 'saldo-pagos', 'martes', 'miercoles', 'jueves', 'viernes', 'FCI', 'saldo con pendientes y FCI']
    df_tablero = pd.DataFrame(index=filas_tablero, columns=columnas).fillna(0.0)

    mapa_bancos = {
        'Bancor': 'PCIA DE CORDOBA', 'Nación': 'NACION', 'ICBC': 'ICBC', 
        'Patagonia': 'PATAGONIA', 'Macro': 'MACRO', 'Galicia': 'GALICIA', 
        'Credicoop': 'CREDICOOP', 'Comafi': 'COMAFI', 'Mercado Pago': 'MERCADO PAGO'
    }
    for b, term in mapa_bancos.items():
        saldo = df_cc[df_cc['Descripcion'].str.contains(term, na=False)]['Saldo fecha'].sum()
        df_tablero.at[b, 'saldo online'] = saldo

    resumen_pagos = df_p[df_p['Proveedor'] != 'TOTAL GENERAL'].groupby('Método de pago')['Saldo'].sum()
    for metodo, monto in resumen_pagos.items():
        if metodo in df_tablero.index:
            df_tablero.at[metodo, 'proveedores pendientes'] = monto

    if not df_otros_pagos.empty:
        total_semana_imp = df_otros_pagos[df_otros_pagos['Concepto'] == 'TOTAL IMPUESTOS']['Semana'].values[0]
        df_tablero.at['Macro', 'otros pagos pendientes'] = total_semana_imp

    mapa_fci = {'Macro': 'MACRO', 'Credicoop': 'CREDICOOP', 'Patagonia': 'PATAGONIA', 'Bancor': 'CORDOBA', 'Galicia': 'GALICIA', 'Comafi': 'COMAFI'}
    for b, term in mapa_fci.items():
        fci_valor = df_fci[df_fci['Descripcion'].str.contains(term, na=False)]['Saldo fecha'].sum()
        df_tablero.at[b, 'FCI'] = fci_valor

    e_lun, e_mar, e_mie, e_jue, e_vie = procesar_valores_por_fecha(df_echeqs, hoy_base)
    cba_lun, cba_mar, cba_mie, cba_jue, cba_vie = procesar_valores_por_fecha(df_cba, hoy_base)
    bsas_lun, bsas_mar, bsas_mie, bsas_jue, bsas_vie = procesar_valores_por_fecha(df_bsas, hoy_base)

    df_tablero.at['Echeq de terceros', 'saldo online'] = e_lun                    
    df_tablero.at['Echeq de terceros', 'martes'] = e_mar 
    df_tablero.at['Echeq de terceros', 'miercoles'] = e_mie                        
    df_tablero.at['Echeq de terceros', 'jueves'] = e_jue
    df_tablero.at['Echeq de terceros', 'viernes'] = e_vie

    df_tablero.at['Cheques CBA', 'saldo online'] = cba_lun
    df_tablero.at['Cheques CBA', 'martes'] = cba_mar
    df_tablero.at['Cheques CBA', 'miercoles'] = cba_mie
    df_tablero.at['Cheques CBA', 'jueves'] = cba_jue
    df_tablero.at['Cheques CBA', 'viernes'] = cba_vie

    df_tablero.at['Cheques BA', 'saldo online'] = bsas_lun
    df_tablero.at['Cheques BA', 'martes'] = bsas_mar
    df_tablero.at['Cheques BA', 'miercoles'] = bsas_mie
    df_tablero.at['Cheques BA', 'jueves'] = bsas_jue
    df_tablero.at['Cheques BA', 'viernes'] = bsas_vie

    df_tablero['saldo-pagos'] = df_tablero['saldo online'] - df_tablero['proveedores pendientes'] - df_tablero['otros pagos pendientes']
    
    df_tablero['saldo con pendientes y FCI'] = (
        df_tablero['saldo-pagos'] + 
        df_tablero['martes'] + 
        df_tablero['miercoles'] +
        df_tablero['jueves'] +
        df_tablero['viernes'] +
        df_tablero['FCI']
    )
    df_tablero.loc['TOTAL'] = df_tablero.sum()
    df_tablero = df_tablero.rename(columns={'saldo online': 'saldo contable'})

    if COLUMNA_FECHA_PROVEEDORES in df_p.columns:
        df_p[COLUMNA_FECHA_PROVEEDORES] = pd.to_datetime(df_p[COLUMNA_FECHA_PROVEEDORES], errors='coerce').dt.strftime('%Y-%m-%d')

    df_p = df_p.fillna('')
    
    filas_valores = ['Echeq de terceros', 'Cheques CBA', 'Cheques BA']
    df_tablero_bancos = df_tablero.drop(index=filas_valores, errors='ignore')
    if 'TOTAL' in df_tablero_bancos.index:
        df_tablero_bancos = df_tablero_bancos.drop(index='TOTAL')
        
    df_tablero_bancos = df_tablero_bancos[['saldo contable', 'proveedores pendientes', 'otros pagos pendientes', 'saldo-pagos', 'FCI', 'saldo con pendientes y FCI']]
    df_tablero_bancos.loc['TOTAL'] = df_tablero_bancos.sum()
    
    df_tablero_cheques = df_tablero.loc[df_tablero.index.isin(filas_valores)].copy()
    df_tablero_cheques.loc['TOTAL CHEQUES'] = df_tablero_cheques.sum() 
    
    df_tablero_cheques.columns = [
        'proveedores pendientes', 
        'otros pagos pendientes', 
        'lunes', 
        'saldo-pagos', 
        'martes', 
        'miércoles', 
        'jueves', 
        'viernes', 
        'FCI', 
        'saldo con pendientes y FCI'
    ]

    df_tablero_cheques = df_tablero_cheques[[
        'proveedores pendientes', 
        'otros pagos pendientes', 
        'saldo-pagos', 
        'lunes', 
        'martes', 
        'miércoles', 
        'jueves', 
        'viernes', 
        'FCI', 
        'saldo con pendientes y FCI'
    ]]

    return df_p, df_tablero_bancos, df_tablero_cheques, df_expansion_echeqs, df_otros_pagos


def escribir_hoja_tablero(writer, nombre_hoja, df_p, df_tablero_bancos, df_tablero_cheques, df_expansion_echeqs, df_otros_pagos):
    df_p.to_excel(writer, sheet_name=nombre_hoja, index=False, startcol=0)
    
    col_inicio_tablero = len(df_p.columns) + 1
    df_tablero_bancos.to_excel(writer, sheet_name=nombre_hoja, startcol=col_inicio_tablero, startrow=0)

    fila_inicio_cheques = len(df_tablero_bancos) + 3
    df_tablero_cheques.to_excel(writer, sheet_name=nombre_hoja, startcol=col_inicio_tablero, startrow=fila_inicio_cheques)

    fila_inicio_expansion = fila_inicio_cheques + len(df_tablero_cheques) + 3
    df_expansion_echeqs.to_excel(writer, sheet_name=nombre_hoja, startcol=col_inicio_tablero, startrow=fila_inicio_expansion, index=False)

    fila_inicio_impuestos = fila_inicio_expansion + len(df_expansion_echeqs) + 3
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
    fmt_naranja_titulo = workbook.add_format({'bg_color': "#E67E22", 'font_color': 'white', 'bold': True, 'border': 1})

    for col_num, value in enumerate(df_p.columns.values):
        worksheet.write(0, col_num, value, fmt_azul)
        
    for r in range(1, len(df_p) + 1):
        es_total_cruce = (str(df_p.iloc[r-1].get('Proveedor', '')) == 'TOTAL GENERAL' or 
                          str(df_p.iloc[r-1].iloc[0]) == 'TOTAL GENERAL')
        
        fmt_fila = fmt_gris_total if es_total_cruce else (fmt_amarillo_pastel if r % 2 == 0 else fmt_blanco)
        
        for c in range(len(df_p.columns)):
            val = df_p.iloc[r-1, c]
            if isinstance(val, (int, float)) and val != '':
                worksheet.write(r, c, float(val), fmt_fila)
            else:
                worksheet.write(r, c, str(val), fmt_fila)

    worksheet.write(0, col_inicio_tablero, "Concepto", fmt_verde)
    for col_num, value in enumerate(df_tablero_bancos.columns.values):
        worksheet.write(0, col_inicio_tablero + col_num + 1, value, fmt_verde)

    worksheet.write(fila_inicio_cheques, col_inicio_tablero, "Concepto", fmt_rojo)
    for col_num, value in enumerate(df_tablero_cheques.columns.values):
        worksheet.write(fila_inicio_cheques, col_inicio_tablero + col_num + 1, value, fmt_rojo)

    for col_num, value in enumerate(df_expansion_echeqs.columns.values):
        worksheet.write(fila_inicio_expansion, col_inicio_tablero + col_num, value, fmt_naranja_titulo)

    for col_num, value in enumerate(df_otros_pagos.columns.values):
        worksheet.write(fila_inicio_impuestos, col_inicio_tablero + col_num, value, fmt_gris_oscuro)

    for r in range(1, len(df_tablero_bancos) + 1):
        fmt_fila = fmt_amarillo_pastel if r % 2 == 0 else fmt_blanco
        if df_tablero_bancos.index[r-1] == 'TOTAL': 
            fmt_fila = fmt_gris_total
        
        worksheet.write(r, col_inicio_tablero, df_tablero_bancos.index[r-1], fmt_fila)
        for c in range(len(df_tablero_bancos.columns)):
            worksheet.write(r, col_inicio_tablero + c + 1, df_tablero_bancos.iloc[r-1, c], fmt_fila)

    for r in range(1, len(df_tablero_cheques) + 1):
        fila_excel = fila_inicio_cheques + r
        fmt_fila = fmt_amarillo_pastel if r % 2 == 0 else fmt_blanco
        if df_tablero_cheques.index[r-1] == 'TOTAL CHEQUES': 
            fmt_fila = fmt_gris_total
            
        worksheet.write(fila_excel, col_inicio_tablero, df_tablero_cheques.index[r-1], fmt_fila)
        for c in range(len(df_tablero_cheques.columns)):
            worksheet.write(fila_excel, col_inicio_tablero + c + 1, df_tablero_cheques.iloc[r-1, c], fmt_fila)

    for r in range(1, len(df_expansion_echeqs) + 1):
        fila_excel = fila_inicio_expansion + r
        fmt_fila = fmt_amarillo_pastel if r % 2 == 0 else fmt_blanco
        
        concepto_exp = str(df_expansion_echeqs.iloc[r-1, 0])
        if concepto_exp == 'TOTAL EXPANSION':
            fmt_fila = fmt_gris_total
            
        worksheet.write(fila_excel, col_inicio_tablero, concepto_exp, fmt_fila)
        
        for c in range(1, len(df_expansion_echeqs.columns)):
            monto_val = df_expansion_echeqs.iloc[r-1, c]
            try:
                worksheet.write(fila_excel, col_inicio_tablero + c, float(monto_val), fmt_fila)
            except (ValueError, TypeError):
                worksheet.write(fila_excel, col_inicio_tablero + c, str(monto_val), fmt_fila)

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

    p_act, ban_act, chq_act, exp_act, imp_act = calcular_datos_tablero(hoy_actual)
    p_ent, ban_ent, chq_ent, exp_ent, imp_ent = calcular_datos_tablero(hoy_entrante)

    nombre_archivo = f'Reporte_Pagos_{datetime.now().strftime("%Y%m%d")}.xlsx'

    with pd.ExcelWriter(nombre_archivo, engine='xlsxwriter') as writer:
        escribir_hoja_tablero(writer, 'Semana en curso', p_act, ban_act, chq_act, exp_act, imp_act)
        escribir_hoja_tablero(writer, 'Semana entrante', p_ent, ban_ent, chq_ent, exp_ent, imp_ent)

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