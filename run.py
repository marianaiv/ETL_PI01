'''
Dataset:
'''

import argparse                         # opciones para script
import pandas as pd                     # dataframes y manejo de datos
import unidecode                        # manejo de encodings
import numpy as np                      # arrays
import os                               # manejo de paths
from sqlalchemy import create_engine    # conexion a sql
import datetime                         # reconocer datetime data
import getpass                          # obtener el password 


def cargar_archivo(path, filename, separator = ','):
    '''Carga el archivo a dataframe dependiendo de
    la extension del archivo'''

    extension = filename.split('.')[1]
    filepath = os.path.join(path,filename)
    
    if extension == 'csv' or extension == 'txt':
        df = pd.read_csv(filepath, sep = separator)
    elif extension == 'json':
        df = pd.read_json(filepath)
    elif extension == 'xlsx':
        df = pd.read_excel(filepath)
    elif extension == 'parquet':
        df = pd.read_parquet(filepath)        

    return df

def main():

    # Opciones para correr el script
    parser = argparse.ArgumentParser(description="Opciones para correr el script")
    parser.add_argument('--archivo', type=str, help='Nombre del archivo a procesar y agregar. Ej: precios_semana_20201123.csv')
    parser.add_argument('--db', type=str, default='ETL_project', help='Nombre de la base de datos. Default: ETL_project')
    parser.add_argument('--sep', type=str, default=',', help='Separador del archivo')

    flags = parser.parse_args()

    # Conexión a mysql
    CLAVE = getpass.getpass('Password de MySql: ')
    DB = flags.db
    my_conn = create_engine("mysql://root:{clave}@localhost/{db}".format(clave=CLAVE,db=DB))
    # postgres
    #engine = create_engine('postgresql://user@localhost:5432/mydb')
    
    ########################
    ###### EXTRACCIÓN ######
    ########################
    
    FILENAME = flags.archivo # ejemplo: precios_semana_20200413.csv
    PATH = 'datasets/'
    SEPARATOR = flags.sep

    # cargamos el archivo a un dataframe
    df = cargar_archivo(PATH, FILENAME, SEPARATOR)
    
    ########################
    #### TRANSFORMACION ####
    ########################

    # Eliminamos las columnas que tengan nulos en sucursal_id y producto_id
    index_nulls = df.loc[(df.producto_id.isnull()) | (df.sucursal_id.isnull())].index.to_list()
    df.drop(index_nulls, axis='index', inplace=True)

    # Agregamos id de semana
    # El id lo obtenemos del nombre del archivo 
    semana = FILENAME.split('_')[2].split('.')[0]

    df['semanaId'] = semana

    # Formateamos el producto id en caso de que esté en formato float
    if df.producto_id.astype(str).str.contains('.', na=True).sum() > 0:
        df.producto_id = df.producto_id.astype(str).str.replace('.0', '', regex=False)
        df.producto_id = df.producto_id.str.zfill(13)

    # Formateamos el sucursal id si está en formato timestamp
    if df.sucursal_id.str.contains('00:00:00', na=True).sum() > 0:
        df.loc[df.sucursal_id.str.contains('00:00:00', na=True), 'sucursal_id'] = 'Sin dato'

    # Mapeamos el nuevo producto id a la tabla
    query = "SELECT * FROM producto_auxiliar"
    prod_aux = pd.read_sql(query,my_conn)
    df = pd.merge(df, prod_aux, left_on = 'producto_id', right_on = 'antiguoId', how ='left')
    df.drop(['producto_id','antiguoId'], axis='columns', inplace=True)
    
    # Obtenemos el ultimo id en producto
    query = "SELECT precioId FROM precios_1 ORDER BY productoId DESC LIMIT 1"
    last_id = pd.read_sql(query,my_conn).iloc[0,0]
    

    # Agregamos los indices
    df.insert(0, 'precioId', range(last_id+1, last_id +1+ len(df)))

    # Manejo de nulos
    df_aux = df.loc[(df.precio.isnull())].copy()
    df_aux['tipoError'] = 0

    df_aux = pd.concat([df_aux, df.loc[df.productoId.isnull()].copy()])
    df_aux.loc[df_aux.productoId.isnull(), 'tipoError'] = 1

    # Los dropeamos
    index = df.loc[(df.precio.isnull()) | (df.productoId.isnull())].index
    df.drop(index, axis='index', inplace=True)
   
    # Detalles 
    df.rename({'sucursal_id':'sucursalId'}, axis='columns', inplace=True)
    df.productoId = df.productoId.astype(int)
    df_aux.rename({'sucursal_id':'sucursalId'}, axis='columns', inplace=True)
    df_aux.tipoError = df_aux.tipoError.astype(int)

    df = df[['precioId','precio','productoId','sucursalId','semanaId']]

    print('Los datos fueron normalizados.')

    ########################
    ######## CARGA #########
    ########################
    
    # Cargo los precios a la tabla precios del db
    df.to_sql(con=my_conn, name='precios_1', if_exists='append', index=False)

    # Cargo los datos a la tabla aux del db
    df_aux.to_sql(con=my_conn, name='precios_auxiliar', if_exists='append', index=False)

    print('Los datos fueron correctamente agregados a la base de datos.')
    #print(df.head())

    #print(df_aux.head())

if __name__ == "__main__":
    main()