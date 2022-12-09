from fastapi import FastAPI,Path,UploadFile,File
from typing import Optional
from pydantic import BaseModel
import shutil
import pandas as pd
import numpy as np
df_unificado = pd.read_csv('Datasets/data_series_movies.csv')
#cargo de antemano una serie con los valores que voy a necesitar para encontrar los actores
actores = df_unificado[['plataforma','fecha_de_estreno','elenco']]
#al agrupar me quedan juntos todos los elencos asi que los uno y les inserto una , entre medio para poder usarlo como lista despues
actores_lista = actores.groupby(['plataforma','fecha_de_estreno'])['elenco'].apply(lambda x: ','.join(x))

app = FastAPI()
@app.get('/get_max_duration/')
async def get_max_duration(anio:int, plataforma:str, minOSeason:str):
  '''
  Máxima duración según tipo de film (película/serie), por plataforma y por año
  '''
  message = f"La pelicula o serie con mas duracion en {plataforma} es"
  #separo los que tienen el tipo de tiempo requerido y agarro los datos que voy a usar
  max_lists = df_unificado.loc[df_unificado['duracion_minOSeason'] == minOSeason][['titulo','plataforma','fecha_de_estreno','duracion_cantidad']]
  #transformo la columna que contiene la cantidad de minutos o de seasons en un, ya que al tener algunos datos que son 'sin dato' esta en string
  max_lists['duracion_cantidad'] = max_lists['duracion_cantidad'].astype('int64')
  #agrupo y busco mayor valor de duracion
  max_lists = max_lists.groupby(['plataforma','fecha_de_estreno','titulo'])['duracion_cantidad'].max()
  try:
    #si existe el dato, es decir si la plataforma existe y tiene el tipo de tiempo indicado podra hacer esta operacion sino dara un mensaje de error
    pelicula_o_serie_max = max_lists[plataforma][anio].sort_values(ascending=False).index[0]
    return {message : pelicula_o_serie_max}
  except:
    message = f'no hay series o peliculas en {minOSeason} en la plataforma {plataforma}'
    return {message}

@app.get('/get_count_plataform/')
async def get_count_plataform(plataforma:str):
  '''
  Cantidad de películas y series (separado) por plataforma
  '''
  message = f"en {plataforma} hay "
  #filtro por plataforma y por movie y tv show, y guardo la cantidad de columnas
  peliculas = df_unificado.loc[(df_unificado['plataforma'] == plataforma) & (df_unificado['clasificacion'] == 'movie')].shape[0]
  series = df_unificado.loc[(df_unificado['plataforma'] == plataforma) & (df_unificado['clasificacion'] == 'tv show')].shape[0]
  message = f"en {plataforma} hay {peliculas} peliculas y {series} series"
  return {message}

@app.get('/get_listedin/')
async def get_listedin(genero:str):
  '''
  Cantidad de veces que se repite un género y plataforma con mayor frecuencia del mismo
  '''
  #filtro por genero 
  df_por_genero = df_unificado.loc[(df_unificado['lista_de_generos'].str.contains(genero))]
  #calculo cuantos datos hay de cada plataforma
  plat_mas_genero = df_por_genero['plataforma'].value_counts()
  #guardo el nombre de la plataforma (estaba en el index) y la cantidad de veces que esta ese genero 
  plat_mas_genero = plat_mas_genero[plat_mas_genero == plat_mas_genero.max()].index[0]
  cant_rep_genero = df_por_genero['plataforma'].value_counts().max()
  
  message = f'la plataforma con mas series o peliculas del genero {genero} es {plat_mas_genero} con {cant_rep_genero} repeticiones'
  return {message}

@app.get('/get_actor/')
async def get_actor(plataforma:str, anio:int):
  '''
  Actor que más se repite según plataforma y año
  '''
  message = f"El actor mas repetido en {plataforma} es"
  #actores_lista es un string con datos separados por comas, al aplicar un split lo transformo en una lista
  actores_lista_filtro = actores_lista[plataforma][anio].split(',')
  #creo un set con los nombres sin repetir de los actores que voy a evaluar
  actores_unicos = set(actores_lista_filtro)
  #aca voy a guardar al actor mas repetido junto con las veces que aparece
  actor_mas_repetido = ('',0)
  for element in actores_unicos:
    #saco los nulos
    if element != 'sin dato':
      if actores_lista_filtro.count(element) > actor_mas_repetido[1] :
        actor_mas_repetido = (element,actores_lista_filtro.count(element))
  return {message : actor_mas_repetido[0]}