'''
En este script se generan las tablas para armar el database.
'''
import pandas as pd
import unidecode
import numpy as np

### Sucursal ###
suc = pd.read_csv('../datasets/sucursal.csv')

# Tabla de comercio-bandera
df_bandera = suc[['banderaId','comercioId', 'banderaDescripcion', 'comercioRazonSocial']].copy()
df_bandera.drop_duplicates(inplace=True)
df_bandera.reset_index(inplace=True, drop=True)
# agrego un id
df_bandera.insert(0, 'comercioBanderaId', range(1, 1 + len(df_bandera)))

# Mapeamos el id a sucursal
suc = pd.merge(suc, df_bandera, on = ['banderaId','comercioId', 'banderaDescripcion','comercioRazonSocial'], how='left')
suc.drop(['banderaId','comercioId', 'banderaDescripcion', 'comercioRazonSocial'], axis='columns', inplace=True)

# Tabla tipo de sucursal
df_tipo_suc = suc['sucursalTipo'].copy()
df_tipo_suc.drop_duplicates(inplace=True)
df_tipo_suc = df_tipo_suc.to_frame()
df_tipo_suc.insert(0, 'tipoSucursalId', range(1, 1 + len(df_tipo_suc)))
df_tipo_suc.reset_index(inplace=True, drop=True)

# Mapeamos el id a sucursal
suc = pd.merge(suc, df_tipo_suc, on = ['sucursalTipo'])
suc.drop(['sucursalTipo'], axis='columns', inplace=True)

# tabla de provincia y localidad
df_provincia = suc['provincia'].copy().to_frame()
df_provincia.drop_duplicates(inplace=True)
df_provincia.insert(0, 'provinciaId', range(1, 1 + len(df_provincia)))
df_provincia.reset_index(drop=True,inplace=True)

# tabla de localidad
df_localidad = suc[['provincia','localidad']].copy()
df_localidad.drop_duplicates(subset=['provincia','localidad'], inplace=True)
# arreglamos strings
df_localidad.localidad = df_localidad.localidad.str.strip().str.capitalize()
# Mapeamos los id de provincia
df_localidad = pd.merge(df_localidad, df_provincia, on = ['provincia'], how='left')
df_localidad.drop(['provincia'], axis='columns', inplace=True)
df_localidad.drop_duplicates(subset=['localidad','provinciaId'], inplace=True)
# insertamos un index
df_localidad.insert(0, 'localidadId', range(1, 1 + len(df_localidad)))
df_localidad.reset_index(drop=True,inplace=True)

# Mapeamos localidad a sucursal
df_localidad_map = pd.merge(df_localidad, df_provincia, on = ['provinciaId'], how='left')
# arreglamos string antes de merge
suc.localidad = suc.localidad.str.strip().str.capitalize()
#juntamos
suc = pd.merge(suc, df_localidad_map, on = ['provincia','localidad'], how='left', validate = 'm:1')
suc.drop(['provincia','localidad','provinciaId'], axis='columns', inplace=True)

# reordenamos
suc = suc[['id','sucursalNombre','comercioBanderaId','tipoSucursalId','localidadId', 'direccion','lat','lng']]

# strings de nombre y direccion
suc.sucursalNombre = suc.sucursalNombre.str.strip().str.capitalize()
suc.direccion = suc.direccion.str.strip().str.capitalize()

# renombramos columnas
suc.rename(columns={'id': "sucursalId", 'lat': 'latitud', 'lng': 'longitud'}, inplace=True)
suc.reset_index(drop=True, inplace=True)

# guardamos los archivos producto de la transformacion
dfs = [suc, df_localidad, df_provincia, df_bandera, df_tipo_suc]
nombres = ['sucursal_nuevo', 'sucursal_localidad', 'sucursal_provincia', 'sucursal_bandera', 'sucursal_tipo']
for df, nombre in zip(dfs, nombres):
    df.to_csv('../datasets/{}.csv'.format(nombre), index=False)


#### Producto ####
prod = pd.read_parquet('../datasets/producto.parquet')

# espacios en blanco y primera letra mayuscula
prod.marca = prod.marca.str.strip().str.capitalize()

# Eliminamos la presentacion del nombre
prod.nombre = prod.nombre.str.split('(\d+)', expand=True)[0]
# Espacios en blanco y letra mayuscula
prod.nombre = prod.nombre.str.strip().str.capitalize()

# dropeamos columnas inutiles
prod_error_aux = prod.loc[prod.categoria1.notnull()].copy()
prod_error_aux['tipoError'] = 0
# cambiamos el nombre de id
prod_error_aux.rename({'id':'antiguoId'}, axis=1, inplace=True)

# columnas
prod.drop(['categoria1','categoria2','categoria3'],axis='columns', inplace=True)
prod.head()

# filas (coinciden con las filas que se copiaron a aux)
prod.drop(prod.loc[(prod.nombre.isnull()) | (prod.marca.isnull())].index, axis='index', inplace=True)

# Hacemos split y creamos una columna para cantidad y unidad
prod['cantidad'] = prod.presentacion.str.split(expand=True)[0].astype(float)
prod['unidad'] = prod.presentacion.str.split(expand=True)[1]

# Cambiamos unidades 
prod.loc[prod.unidad == 'kg', 'cantidad'] = prod.cantidad * 1000
prod.loc[prod.unidad == 'kg', 'unidad'] = 'gr'

prod.loc[prod.unidad == 'lt', 'cantidad'] = prod.cantidad * 1000
prod.loc[prod.unidad == 'lt', 'unidad'] = 'ml'
prod.loc[prod.unidad == 'cc', 'unidad'] = 'ml'

# juntamos cantidad y unidad y sustituimos presentacion
prod['presentacion'] = prod.cantidad.astype(str) + ' ' + prod.unidad
prod.drop(['cantidad','unidad'], axis='columns', inplace=True)

# Quitamos la marca del nombre
prod['marca_st'] = prod.apply((lambda row : unidecode.unidecode(str(row['marca']).lower())), axis=1)
prod.nombre = prod.apply(lambda row: row['nombre'].replace(str(row['marca_st']),''), axis=1)

# Dropeamos la columna auxiliar
prod.drop('marca_st', axis='columns', inplace=True)

# Para normalizar, hacemos una tabla de marcas
df_marca = prod.marca.to_frame().copy()
df_marca.drop_duplicates(inplace=True)
df_marca.insert(0, 'marcaId', range(1, 1 + len(df_marca)))

# Mapping a producto
prod = pd.merge(prod, df_marca, on=['marca'])
prod.drop('marca', axis=True, inplace=True)
prod.reset_index(drop=True, inplace=True)

# formateamos para que tengan codigos EAN de 13 digitos
prod.loc['id'] = prod['id'].astype(str)
prod['codigoEAN'] = prod.loc[:, 'id']
prod.loc[prod['codigoEAN'].str.len() != 13, 'codigoEAN'] = prod['codigoEAN'].loc[prod['codigoEAN'].str.len() != 13].str.split('-').str[2]

# Vamos a convertir esta tabla en la tabla aux y crear una de productos
# donde los productos no se repitan
prod_aux = prod.copy()

# dropeamos las filas que son del mismo producto
prod.drop_duplicates(subset=['nombre','presentacion','marcaId','codigoEAN'], inplace=True)

# reseteamos indices y agregamos un id
prod.reset_index(inplace=True, drop=True)
prod['productoId'] = prod.index + 1 
prod.productoId.astype(int)
prod = prod[['productoId','nombre','presentacion','marcaId','codigoEAN','id']]

# mapeamos el id a prod aux, para poder eliminar id de prod
prod_aux = pd.merge(prod, prod_aux, on=['nombre', 'presentacion', 'marcaId','codigoEAN'], how = 'right')
prod_aux.drop(['id_x','nombre','presentacion','marcaId','codigoEAN'], axis='columns', inplace = True)
prod_aux.rename({'id_y':'antiguoId'}, axis=1, inplace=True)

# chequeamos que no hayan nulos
prod_aux.loc[(prod_aux.productoId.isnull()) | (prod_aux.antiguoId.isnull())]

# Eliminamos id de la tabla de producto del modelo
prod.drop('id', axis='columns', inplace=True)

# Guardamos los archivos producto de la limpieza
dfs = [prod, df_marca, prod_aux, prod_error_aux]
nombres = ['producto', 'producto_marca', 'producto_auxiliar', 'producto_error']

for df, nombre in zip(dfs, nombres):
    df.to_csv('../datasets/{}.csv'.format(nombre), index=False)

#### Precio inicial ####

precios_0413 = pd.read_csv('../datasets/precios_semana_20200413.csv')

# Dropeamos nulos 
index_nulls = precios_0413.loc[(precios_0413.producto_id.isnull()) | (precios_0413.sucursal_id.isnull())].index.to_list()
precios_0413.drop(index_nulls, axis='index', inplace=True)

# le agregamos el idSemana
filename = 'precios_semana_20200413.csv'
# Lo obtenemos del nombre del archivo
semana = filename.split('_')[2].split('.')[0]

precios_0413['semanaId'] = semana

# Agrego id y reordeno
precios_0413.reset_index(drop=True, inplace=True)
precios_0413['precioId'] = precios_0413.index + 1
precios_0413 = precios_0413[['precioId','precio','producto_id','sucursal_id','semanaId']].copy()
precios_0413.rename({'producto_id':'productoId','sucursal_id':'sucursalId'}, axis='columns', inplace=True)

# cambio los indices de producto por los nuevos
prod_aux = pd.read_csv('../datasets/producto_auxiliar.csv') #se genera en transformaciones_producto.ipynb
precios_0413 = pd.merge(precios_0413, prod_aux, left_on = 'productoId', right_on = 'antiguoId', how = 'left')
precios_0413.drop(['productoId_x','antiguoId'], axis=1, inplace=True)
precios_0413.rename({'productoId_y':'productoId'}, axis='columns', inplace=True)
precios_0413 = precios_0413[['precioId','precio','productoId','sucursalId','semanaId']].copy()

# Guardo a un csv
precios_0413.to_csv('../datasets/precios_0413.csv', index=False)
