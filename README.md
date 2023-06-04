
# transformar_datos_accidentes

## Problema a resolver

Se realiza la extracción, limpieza y transformación de datos provenientes de la siguiente API del gobierno (https-datos.gov.co/resource/7cci-nqqb.json) la cual contiene datos de algunos incidentes de tránsito que ocurrieron en algunas zonas del país. Finalmente, se exportan los datos a un archi *.csv* y las tablas creadas a partir de los datos se exportan a un archivo excel y a una base de datos de sqlite.

## Descripción de la solución

Se importan las librerias

```
import sqlite3
from datetime import datetime
import numpy as np
import pandas as pd
    
```

**Extracción**

Se extraen los datos de la API y se almacenan en un dataframe

```
datos = pd.read_json('https://datos.gov.co/resource/7cci-nqqb.json')

df = pd.DataFrame(data=datos)
    
```
**Limpieza**

Se renombre la columna *orden* y se modifica la columna *d_a*

```
df.rename(columns={ 'orden': 'id_accidente' }, inplace = True)

df['d_a'] = [ i[4:] for i in df['d_a'] ]
   
```
Se realiza los ajustes necesario para que la columna fecha almacene la fecha y hora del accidente y el tipo de dato sea *datetime*

```
x=0
j=0
for i in df['hora']:   
    if i[9]=='p' and i[0:2]!='12':
        j=int(i[0:2])+12
        df.loc[x, 'hora'] = str(j)+i[2:8]
    elif i[9]=='a' and i[0:2]=='12':
        df.loc[x, 'hora'] = '00'+i[2:8]
    else:
        df.loc[x, 'hora'] = i[0:8]
    x=x+1
    
df['fecha'] = df.fecha.str.cat(df.hora, sep=' ')

df['fecha']= df['fecha'].astype('datetime64')
   
```
Se eliminan las columnas que se consideran redundantes

`df.drop(['a_o', 'mes', 'hora'], axis = 1, inplace = True)`

**Transformación**

A partir de la columna *nombrecomuna* se genera la columna *id_comuna*

```
df['nombrecomuna'] = np.where(df['nombrecomuna']=='SIN INFORMACION', '25. SIN INFORMACION',df['nombrecomuna'])
df['nombrecomuna'] = np.where(df['nombrecomuna']=='FLORIDABLANCA', '26. FLORIDABLANCA',df['nombrecomuna'])
df['id_comuna']=[ 'C'+i[0:2] for i in df['nombrecomuna'] ]
df['nombrecomuna']=[ i[3:] for i in df['nombrecomuna'] ]
   
```
Se reorganiza las columnas

```
df = df.reindex(columns = ['id_accidente', 'fecha', 'd_a', 'gravedad', 'peaton', 'automovil', 
       'campaero','camioneta', 'micro', 'buseta', 'bus', 'camion', 'volqueta', 'moto',
       'bicicleta', 'otro', 'via_1', 'barrio', 'entidad',  'id_comuna', 'nombrecomuna',
       'propietario_de_veh_culo', 'diurnio_nocturno', 'hora_restriccion_moto'])
   
```
Se exportan los datos a un archivo *.csv*

`df.to_csv("datos_modificados.csv", index=False, encoding = "utf-8-sig")`

A partir de los datos modificados se crearan otras tablas.

Se crea la tabla accidentes

```
    tabla_accidentes = df[['id_accidente', 'fecha', 'via_1', 'barrio', 'entidad',  'id_comuna',
       'propietario_de_veh_culo', 'diurnio_nocturno', 'hora_restriccion_moto']]

```
Se crea la tabla comunas

```
tabla_comunas = df[['id_comuna', 'nombrecomuna']]
tabla_comunas = tabla_comunas.drop_duplicates()
   
```

Se crea la tafla afectados

```
lista_id_afectados=[]
for i in range(12):
    lista_id_afectados.append('AF'+str(i))
    
afectados = ['peaton', 'automovil', 'campaero','camioneta', 'micro', 'buseta', 'bus', 'camion', 'volqueta', 'moto',
       'bicicleta', 'otro']

data_afectados = {
    'id_afectado': lista_id_afectados,
    'afectado': afectados
}

tabla_afectados=pd.DataFrame(data_afectados)
   
```

Se crea la tabla detalle_accidentes

```
dt_afectados=[]

for i in range(12):
    dt_afectados.append(df[df[afectados[i]]!=0])
    dt_afectados[i].insert(1, 'id_afectado', lista_id_afectados[i])
    dt_afectados[i] = dt_afectados[i][['id_accidente','id_afectado',afectados[i]]]
    dt_afectados[i].rename(columns={ afectados[i]: 'cantidad_afectados' }, inplace = True)

tabla_detalle_accidentes = pd.concat(dt_afectados, axis=0)

```
Las tablas se exportan a un archivo excel

```
with pd.ExcelWriter('tablas.xlsx') as writer:
    tabla_accidentes.to_excel(writer, sheet_name='accidentes', index=False)  
    tabla_afectados.to_excel(writer, sheet_name='afectados', index=False)
    tabla_detalle_accidentes.to_excel(writer, sheet_name='detalle_accidentes',     index=False)
    tabla_comunas.to_excel(writer, sheet_name='comunas', index=False)
   
```
Las tablas se exportan a una base de datos se sqlite

```
conexion = sqlite3.connect('accidentes.db')

cursor = conexion.cursor()

tabla_accidentes.to_sql('accidentes', conexion, if_exists='fail',index=False)
tabla_afectados.to_sql('afectados', conexion, if_exists='fail',index=False)
tabla_detalle_accidentes.to_sql('detalle_accidentes', conexion, if_exists='fail',index=False)
tabla_comunas.to_sql('comunas', conexion, if_exists='fail',index=False)

conexion.commit()
conexion.close()
   
```

**Autor**

Angie Rincón
