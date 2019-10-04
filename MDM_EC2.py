from initialize import log, config, s3, FECHA, ENVIRONMENT, CARGA_FULL, primera_carga
import pandas as pd
import data_process
import database
import os

def Carga(archivo):
    # Se lee el archivo csv en un pandas dataframe
    df = pd.read_csv(archivo + '.csv', dtype=str)
    df.set_index('codsap')
    df.fillna('', inplace=True)
    lista_aux = config['sourcesSelect'][archivo].split()
    lista_aux.insert(0,'grupodeatributos')
    name_aux = df.columns[0]
    df = df.rename(columns={name_aux:'grupodeatributos'})
    df = df[lista_aux]
    for col in df.columns:
        df.loc[:,col] = df.loc[:,col].apply(lambda x: str(x).strip())
    # log.info("Archivo %s leido", archivo)
    return df


def Reglas(df, archivo):
    # Limpieza segun archivo
    if(archivo == 'comunicaciones'):
        df_final = data_process.comunicaciones(df, archivo)
    elif(archivo == 'sap'):
        df_final = data_process.sap(df, archivo)
    elif(archivo == 'webredes'):
        df_final = data_process.webredes(df, archivo)
    else:
        df_final = data_process.brandinnovation(df, archivo)
    # log.info("Data de %s preparada", archivo)
    os.remove(archivo + '.csv')
    return df_final


# Detectar que archivos han subido
subidos = dict()
realiza_trabajo = False
log.info(config['MISC']['separador'].format('Inicio') + ';')
lista_archivos = config['Credenciales_'+ENVIRONMENT]['ARCHIVOS'].split()
for archivo in lista_archivos:
    try:
        carpeta = config[ENVIRONMENT+"_Path"][archivo]
        ruta = carpeta + "-" + FECHA + ".csv"
        s3.download_file(config['Credenciales_'+ENVIRONMENT]
                         ['S3_BUCKET'], ruta, archivo+'.csv')
        subidos[archivo] = True
        log.info("Archivo %s encontrado ;", archivo)
    except Exception as error:
        subidos[archivo] = False
        log.info("Archivo %s no encontrado ;", archivo)

# Por cada archivo, aplicar reglas e insertar
for archivo in lista_archivos:
    if subidos[archivo]:
        realiza_trabajo = True
        log.info(config['MISC']['separador'].format(archivo)+';')
        Data_init = Carga(archivo)
        database.Redshift(Data_init, archivo, CARGA_FULL)
        Data_fin = Reglas(Data_init, archivo)
        database.Mongo(Data_fin, archivo, CARGA_FULL, primera_carga)
        primera_carga = False

# Mongo FULL
if CARGA_FULL and realiza_trabajo:
    database.Mongo_Rename()

log.info(config['MISC']['separador'].format('Terminado')+';')