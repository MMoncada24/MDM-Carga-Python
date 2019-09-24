from initialize import log, config
import pandas as pd
import math
import re

# Limpieza Brand Innovation
def brandinnovation(df, archivo):
    lista_c = config['sourcesSelect'][archivo].split()
    df = df[lista_c]
    lista_to_have = config['fillColumns']['columnsToHave'].split()
    lista_diff = [x for x in lista_to_have if x not in df.columns]
    for column in lista_diff:
        df[column] = ""
    return df

# Limpieza Comunicaciones
def comunicaciones(df, archivo):
    df2 = df[['desmaterialmedidacata# logo']].copy()
    df2.apply(lambda x: x[:1500])
    df['desmaterialmedidacata# logo'] = df2['desmaterialmedidacata# logo']
    lista_c = config['sourcesSelect'][archivo].split()
    df = df[lista_c]
    medidas = []
    for row in df.itertuples(index=False):
        material = row.desmaterialmedidacata# logo
        if('Medidas' in material):
            medidas.append(material[material.index('Medidas'):])
        else:
            medidas.append('')
    df['tallaprevia'] = medidas
    lista_to_have = config['fillColumns']['columnsToHave'].split()
    lista_diff = [x for x in lista_to_have if x not in df.columns]
    for column in lista_diff:
        df[column] = ""
    return df

# Limpieza SAP
def sap(df, archivo):
    lista_s = config['sourcesSelect'][archivo].split()
    df = df[lista_s]
    df['desnombreproducto'] = df.loc[:, 'desproductosap']
    ids = list()
    for row in df.itertuples(index=False):
        idstring = str(row.codsap)+str(row.desproductosap)+' '+str(row.desgrupoarticulo)
        ids.append(idstring)
    df['id'] = ids
    lista_have = config['fillColumns']['columnsToHave'].split()
    lista_diff_s = [x for x in lista_have if x not in df.columns]
    for column in lista_diff_s:
        df[column] = ""
    return df

# Limpieza WebRedes
def webredes(df, archivo):
    df2 = df[['destip001', 'destip002', 'destip003', 'destip004', 'despaso001', 'despaso002', 'despaso003', 'despaso004', 'deswebredes',
              'desdescubremas001', 'desdescubremas002', 'destitulotip001', 'destitulotip002', 'destitulotip003', 'destitulotip004']].copy()
    df2.apply(lambda x: x[:1500])
    df = df.assign(destip001=df2['destip001'], destip002=df2['destip002'], destip003=df2['destip003'],
                   destip004=df2['destip004'], despaso001=df2['despaso001'], despaso002=df2['despaso002'],
                   despaso003=df2['despaso003'], despaso004=df2['despaso004'], deswebredes=df2['deswebredes'],
                   desdescubremas001=df2['desdescubremas001'], desdescubremas002=df2['desdescubremas002'],
                   destitulotip001=df2['destitulotip001'], destitulotip002=df2['destitulotip002'],
                   destitulotip003=df2['destitulotip003'], destitulotip004=df2['destitulotip004'])
    lista_w = config['sourcesSelect'][archivo].split()
    df = df[lista_w]
    df3 = df[['deslinkvideo001', 'deslinkvideo002', 'deslinkvideo003',
              'deslinkvideo004', 'deslinkvideo005']].copy()
    df3.apply(lambda x: youtube(x))
    df = df.assign(deslinkvideoId001=df3['deslinkvideo001'], deslinkvideoId002=df3['deslinkvideo002'],
                   deslinkvideoId003=df3['deslinkvideo003'], deslinkvideoId004=df3['deslinkvideo004'],
                   deslinkvideoId005=df3['deslinkvideo005'], desnombreproducto=df.loc[:, 'desnombreproductowebredes'])

    lista_has = config['fillColumns']['columnsToHave'].split()
    lista_diff_w = [x for x in lista_has if x not in df.columns]
    for column in lista_diff_w:
        df[column] = ""
    return df

# Filtro Youtube
def youtube(url):
    youtube_regex = (
        r'(https?://)?(www\.)?(youtube|youtu|youtube-nocookie)\.(com|be)/(watch\?v=|embed/|v/|.+\?v=)?([^&=%\?]{11})')
    youtube_regex_match = re.search(youtube_regex, str(url))
    if youtube_regex_match:
        return str(youtube_regex_match.group())
    else:
        return str(url)


def preparar_dataframe(df, df_old, option, bd, archivo):
    # Atajo
    if df_old.empty and option=='actualizar':
        return pd.DataFrame(columns=df.columns)

    # Identificar registros a insertar y actualizar
    saps_new = df.loc[:, 'codsap']
    saps_old = df_old.loc[:, 'codsap']
    saps_match = saps_new.isin(saps_old)
    saps_insertar = []
    saps_actualizar = []
    index = 0
    for match in saps_match:
        if match:
            saps_actualizar.append(saps_new.iloc[index])
        else:
            saps_insertar.append(saps_new.iloc[index])
        index += 1

    if str(option) == 'insertar':
        # Crear dataframe a insertar
        frame_ins = {'codsap': saps_insertar}
        df_saps_insertar = pd.DataFrame(frame_ins).astype(str)
        df_insertar = pd.merge(df, df_saps_insertar, how='inner', on='codsap').sort_values(by=['codsap'])
        # log.info("DataFrame a insertar listo")
        return df_insertar
    elif str(option) == 'actualizar':
        # Separar data a actualizar
        frame_act = {'codsap': saps_actualizar}
        df_saps_actualizar = pd.DataFrame(frame_act).astype(str)
        df_new_actualizable = pd.merge(
            df, df_saps_actualizar, how='inner', on='codsap').sort_values(by=['codsap'])
        df_old_actualizable = pd.merge(
            df_old, df_saps_actualizar, how='inner', on='codsap').sort_values(by=['codsap'])
        df_new_actualizable = df_new_actualizable.reset_index(drop=True)
        if not df_old_actualizable.empty:
            df_old_actualizable = df_old_actualizable.reset_index(drop=True)
        if str(bd) == 'Mongo':
            df_new_actualizable.insert(
                loc=0, column='_id', value=df_old_actualizable.loc[:, '_id'])
        for colu in df_new_actualizable:
            if colu not in df_old_actualizable:
                df_old_actualizable[colu] = df_new_actualizable.loc[:, colu]

        # Iterar y crear el dataframe a actualizar
        array_actualizar = []
        nuevo_registro = False
        iterador_aux = df_old_actualizable.itertuples(index=False)
        # ticks = manager.counter(total=len(df_new_actualizable.index), 
        #   desc='Dataframe de '+archivo+' en '+bd,unit='registros',color=config['MISC'][archivo+'_color'])
        for fila in df_new_actualizable.itertuples(index=False):
            # ticks.update()
            tupla_old = next(iterador_aux)
            registro = dict()
            for columna_num in range(0, len(df_old_actualizable.columns)):
                if fila[columna_num] == '':
                    registro[df_new_actualizable.columns[columna_num]] = tupla_old[columna_num]
                else:
                    registro[df_new_actualizable.columns[columna_num]] = fila[columna_num]
                    if str(fila[columna_num]) != str(tupla_old[columna_num]):
                        try:
                            a = float(fila[columna_num])
                            b = float(tupla_old[columna_num])
                            if math.isclose(a,b,rel_tol=1e-5):
                                pass
                        except:
                            nuevo_registro = True
            if nuevo_registro:
                array_actualizar.append(registro)
                nuevo_registro = False
        df_actualizar = pd.DataFrame(array_actualizar)
        # ticks.close()
        if df_actualizar.empty:
            df_actualizar = pd.DataFrame(columns=df.columns)
        df_actualizar = df_actualizar[df.columns]
        # log.info("DataFrame a actualizar listo")
        return df_actualizar
