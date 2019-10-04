from initialize import log, config, s3, ENVIRONMENT, FECHA
from data_process import preparar_dataframe
from psycopg2 import sql
from os import remove, getcwd
import pandas as pd
import psycopg2
import pymongo

def Mongo_Rename():
    clientRename = pymongo.MongoClient(config['Credenciales_'+ENVIRONMENT]['MONGO_WRITE_URL'])
    col = clientRename[config['Credenciales_'+ENVIRONMENT]['MONGO_DB']]['staging_MDM_FULL']
    col_bk = clientRename[config['Credenciales_'+ENVIRONMENT]['MONGO_DB']][config['Credenciales_'+ENVIRONMENT]['MONGO_COLLECTION']]
    clientRename[config['Credenciales_'+ENVIRONMENT]['MONGO_DB']].drop_collection('bk_MDM_'+FECHA)
    clientRename[config['Credenciales_'+ENVIRONMENT]['MONGO_DB']].drop_collection('staging_MDM_FULL')
    col_bk.rename('bk_MDM_'+FECHA)
    col.rename(config['Credenciales_'+ENVIRONMENT]['MONGO_COLLECTION'])
    col.create_index('codsap')
    clientRename.close()

def arreglar(cadena, largo):
    cadena_bytes = bytes(cadena,'utf-8')
    if len(cadena_bytes) > largo:
        return cadena_bytes[:largo].decode('utf-8','ignore')
    return cadena

def Redshift(df, archivo, full):
    # Leer situacion del Redshift
    lista_redshift = config['redshiftConnection'][ENVIRONMENT].split()
    con = psycopg2.connect(dbname=lista_redshift[0], host=lista_redshift[1],
                           port=lista_redshift[2], user=lista_redshift[3], password=lista_redshift[4],
                           options='-c search_path='+lista_redshift[5])
    cursor = con.cursor()
    # log.info("Conexion a Redshift establecida")

    # Convertir la consulta en DataFrame
    query_data = "SELECT * FROM {}".format(config['redshiftTables'][archivo])
    query_metadata = "SELECT column_name, character_maximum_length AS largo FROM information_schema.columns WHERE table_name='{}' AND table_schema='{}' ORDER BY ordinal_position;".format(
        config['redshiftTables'][archivo], config['Credenciales_'+ENVIRONMENT]['REDSHIFT_SCHEMA'])
    if not full:
        df_old_redshift = pd.read_sql_query(query_data, con)
    else:
        df_old_redshift = pd.DataFrame(columns=df.columns)
    df_redshift_meta = pd.read_sql_query(query_metadata, con)
    df_redshift_meta.fillna('', inplace=True)
    df_old_redshift.fillna('', inplace=True)
    df_old_redshift = df_old_redshift.astype(str)
    df_old_redshift = df_old_redshift[df.columns]
    # log.info("Tabla %s del Redshift leida", archivo)

    # Mantener orden destino para el COPY
    for columna_maximo in df_redshift_meta.itertuples():
        if columna_maximo.largo != '':
            df.loc[:, columna_maximo.column_name] = df.loc[:, columna_maximo.column_name].apply(
                lambda x: arreglar(str(x),int(columna_maximo.largo)))
           
    # Alistar dataframes
    key_ins = "insertar_df_" + archivo + '.csv'
    key_act = "actualizar_df_" + archivo + '.csv'
    df_insertar = preparar_dataframe(df, df_old_redshift, 'insertar', 'RS',archivo)
    df_actualizar = preparar_dataframe(df, df_old_redshift, 'actualizar', 'RS',archivo)

    if not df_insertar.empty:
        # Llevar dataframe a S3
        df_insertar.to_csv(getcwd()+'/'+key_ins, index=None, header=False, encoding='utf-8')
        s3.upload_file(getcwd()+'/'+key_ins, config['Credenciales_'+ENVIRONMENT]['S3_BUCKET'],
                       config['Credenciales_'+ENVIRONMENT]['S3_STAGING_PATH'] + key_ins)
        cursor.execute(sql.SQL("CREATE TEMPORARY TABLE stage_ins(LIKE {})").format(
            sql.Identifier(config['redshiftTables'][archivo])))
        if full:
            cursor.execute("DROP TABLE IF EXISTS MDM_FULL_staging")
            cursor.execute(sql.SQL("CREATE TEMPORARY TABLE MDM_FULL_staging(LIKE {})").format(
                sql.Identifier(config['redshiftTables'][archivo])))
        cursor.execute("COPY stage_ins FROM 's3://" + config['Credenciales_'+ENVIRONMENT]['S3_BUCKET'] + "/" 
            + config['Credenciales_'+ENVIRONMENT]['S3_STAGING_PATH'] + key_ins
            + "' IAM_ROLE 'arn:aws:iam::820233355588:role/"+ config['Credenciales_'+ENVIRONMENT]['ROL_AWS'] +"' CSV;")

    if not df_actualizar.empty:
        # Llevar dataframe a S3
        df_actualizar.to_csv(getcwd()+'/'+key_act, index=None,
                             header=False, encoding='utf-8')
        s3.upload_file(getcwd()+'/'+key_act, config['Credenciales_'+ENVIRONMENT]['S3_BUCKET'],
                       config['Credenciales_'+ENVIRONMENT]['S3_STAGING_PATH'] + key_act)
        cursor.execute(sql.SQL("CREATE TEMPORARY TABLE stage_act(LIKE {})").format(
            sql.Identifier(config['redshiftTables'][archivo])))
        cursor.execute("COPY stage_act FROM 's3://" + config['Credenciales_'+ENVIRONMENT]['S3_BUCKET'] + "/" +
            config['Credenciales_'+ENVIRONMENT]['S3_STAGING_PATH'] + key_act +
            "' IAM_ROLE 'arn:aws:iam::820233355588:role/"+ config['Credenciales_'+ENVIRONMENT]['ROL_AWS'] + "' CSV;")

    # log.info("Dataframes de %s cargados a Tablas staging ", archivo)

    if not df_insertar.empty:
        # Eliminar archivo csv staging
        #s3.delete_object(Bucket=config['Credenciales_'+ENVIRONMENT]['S3_BUCKET'],
                         #Key=config['Credenciales_'+ENVIRONMENT]['S3_STAGING_PATH'] + key_ins)
        remove(getcwd()+'/'+key_ins)
        # Insertar los registros nuevos
        # log.info('Insertando en RS')
        if full:
            cursor.execute(
                "INSERT INTO MDM_FULL_staging SELECT * FROM stage_ins;")
        else:
            cursor.execute(sql.SQL("INSERT INTO {} SELECT * FROM stage_ins;").format(
                sql.Identifier(config['redshiftTables'][archivo])))
        log.info("Insertados %s nuevos registros de %s al Redshift ;",
                 str(len(df_insertar.index)), archivo)
        cursor.execute("DROP TABLE stage_ins;")

    if not df_actualizar.empty:
        # Eliminar archivo csv staging
        #s3.delete_object(Bucket=config['Credenciales_'+ENVIRONMENT]['S3_BUCKET'],
                         #Key=config['Credenciales_'+ENVIRONMENT]['S3_STAGING_PATH'] + key_act)
        remove(getcwd()+'/'+key_act)
        # Quitar los registros viejos
        # log.info('Actualizando en RS')
        cursor.execute(sql.SQL("DELETE FROM {} USING stage_act WHERE {}.codsap = stage_act.codsap;").format(
            sql.Identifier(config['redshiftTables'][archivo]), sql.Identifier(config['redshiftTables'][archivo])))
        cursor.execute(sql.SQL("INSERT INTO {} SELECT * FROM stage_act;").format(
            sql.Identifier(config['redshiftTables'][archivo])))
        log.info("Actualizados %s registros de %s al Redshift ;",
                 str(len(df_actualizar.index)), archivo)
        cursor.execute("DROP TABLE stage_act;")

    # Ajustes de carga full
    if full:
        cursor.execute(sql.SQL("TRUNCATE {}").format(sql.Identifier(config['redshiftTables'][archivo])))
        cursor.execute(sql.SQL("INSERT INTO {} SELECT * FROM MDM_FULL_staging").format(
            sql.Identifier(config['redshiftTables'][archivo])))

    # Cerrar conexion a redshift
    con.commit()
    cursor.close()
    con.close()
    # log.info("Conexion a Redshift cerrada")


def Mongo(df, archivo, full, primera_carga):
    # Leer situacion actual del Mongo
    if full and primera_carga:
        df_old = pd.DataFrame(columns=df.columns)
    else:
        clientRead = pymongo.MongoClient(
            config['Credenciales_'+ENVIRONMENT]['MONGO_READ_URL'])
        # log.info("Conexion a Mongo establecida")
        # log.info("Leyendo la coleccion del Mongo")
        if full:
            old = clientRead[config['Credenciales_'+ENVIRONMENT]['MONGO_DB']]['staging_MDM_FULL'].find()
        else:
            old = clientRead[config['Credenciales_'+ENVIRONMENT]['MONGO_DB']][config['Credenciales_'+ENVIRONMENT]['MONGO_COLLECTION']].find()
        df_old = pd.DataFrame(list(old))
        df_old.fillna('', inplace=True)
        if df_old.empty:
            df_old = pd.DataFrame(columns=df.columns, dtype=str)
        # log.info("Coleccion de Mongo leida para %s", archivo)
        clientRead.close

    # Alistar data frames
    df_insertar = preparar_dataframe(df, df_old, 'insertar', 'Mongo',archivo)
    if full and primera_carga:
        df_actualizar = df_old
    else:
        df_actualizar = preparar_dataframe(df, df_old, 'actualizar', 'Mongo',archivo)

    # Preparar la subida al mongo
    clientWrite = pymongo.MongoClient(config['Credenciales_'+ENVIRONMENT]['MONGO_WRITE_URL'])
    if full:
        if primera_carga:
            clientWrite[config['Credenciales_'+ENVIRONMENT]['MONGO_DB']].drop_collection('staging_MDM_FULL')
        col = clientWrite[config['Credenciales_'+ENVIRONMENT]['MONGO_DB']]['staging_MDM_FULL']
    else:
        col = clientWrite[config['Credenciales_'+ENVIRONMENT]['MONGO_DB']][config['Credenciales_'+ENVIRONMENT]['MONGO_COLLECTION']]

    # Escribir los nuevos registros directo
    if not df_insertar.empty:
        # log.info('Insertando en Mongo')
        col.insert_many(df_insertar.to_dict('records'))
        log.info("Insertados %s nuevos documentos al Mongo ;",
                 str(len(df_insertar.index)))

    # Iterar el segundo DF y actualizar los viejos docs con replace_one
    if not df_actualizar.empty:
        # log.info('Actualizando en Mongo')
        bulk = col.initialize_unordered_bulk_op()
        for indice_actualizar, registro_actualizar in df_actualizar.iterrows():
            registro = dict(registro_actualizar)
            registro.pop('_id', None)
            bulk.find({'codsap': registro_actualizar['codsap']}).replace_one(
                registro)
        bulk.execute()
        log.info("Actualizados %s documentos del Mongo ;", indice_actualizar)

    clientWrite.close
    # log.info("Conexion a Mongo cerrada")
