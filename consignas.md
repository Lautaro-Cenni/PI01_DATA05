<p align=center><img src=https://d31uz8lwfmyn8g.cloudfront.net/Assets/logo-henry-white-lg.png><p>

# <h1 align=center> **PROYECTO INDIVIDUAL Nº1** </h1>

# <h1 align=center>**`Data Engineering`**</h1>

<p align="center">
<img src="https://files.realpython.com/media/What-is-Data-Engineering_Watermarked.607e761a3c0e.jpg"  height=300>
</p>

¡Bienvenidos al primer proyecto individual de la etapa de labs! En esta ocasión, deberán hacer un trabajo situándose en el rol de un ***Data Engineer***.  

<hr>  

## **Introducción**

La idea de este proyecto es que el alumno logre internalizar los conocimientos requeridos para la elaboración y ejecución de una API.

`Application Programming Interface` es una interfaz que permite que dos aplicaciones se comuniquen entre sí, independientemente de la infraestructura subyacente. Son herramientas muy versátiles y fundamentales para la creación de, por ejemplo, pipelines, ya que permiten mover y brindar acceso simple a los datos que se quieran disponibilizar a través de los diferentes endpoints, o puntos de salida de la API.

Hoy en día contamos con **FastAPI**, un web framework moderno y de alto rendimiento para construir APIs con Python.
<p align=center>
<img src = 'https://i.ibb.co/9t3dD7D/blog-zenvia-imagens-3.png' height=250><p>

## **Propuesta de trabajo**

El proyecto consiste en realizar una ingesta de datos desde diversas fuentes, posteriormente aplicar las transformaciones que consideren pertinentes, y luego disponibilizar los datos limpios para su consulta a través de una API. Esta API deberán construirla en un entorno virtual dockerizado.

Los datos serán provistos en archivos de diferentes extensiones, como *csv* o *json*. Se espera que realicen un EDA para cada dataset y corrijan los tipos de datos, valores nulos y duplicados, entre otras tareas. Posteriormente, tendrán que relacionar los datasets así pueden acceder a su información por medio de consultas a la API.

Las consultas a realizar son:

+ Máxima duración según tipo de film (película/serie), por plataforma y por año:
    El request debe ser: get_max_duration(año, plataforma, [min o season])

+ Cantidad de películas y series (separado) por plataforma
    El request debe ser: get_count_plataform(plataforma)  
  
+ Cantidad de veces que se repite un género y plataforma con mayor frecuencia del mismo.
    El request debe ser: get_listedin('genero')  
    Como ejemplo de género pueden usar 'comedy', el cuál deberia devolverles un cunt de 2099 para la plataforma de amazon.

+ Actor que más se repite según plataforma y año.
  El request debe ser: get_actor(plataforma, año)

## **Pasos del proyecto**

1. Ingesta y normalización de datos

2. Relacionar el conjunto de datos y crear la tabla necesaria para realizar consultas. Aquí se recomienda corroborar qué datos necesitarán en base a las consultas a realizar y concatenar las 4 tablas

3. Leer documentación en links provistos e indagar sobre Uvicorn, FastAPI y Docker

4. Crear la API en un entorno Docker → leer documentación en links provistos

5. Realizar consultas solicitadas

6. Realizar un video demostrativo

7. **PLUS**: realizar un deployment en Mogenius 

<p align=center>
<img src = 'https://i.postimg.cc/2SwvnTcw/Sin-t-tulo.png' height = 400></p>

## **Criterios de evaluación**

**`Código`**:

+ Prolijidad de código 

+ Uso de clases y/o funciones, en caso de ser necesario

+ Código comentado

**`Repositorio`**:

+ Nombres de archivo adecuados

+ Uso de carpetas para ordenar los archivos

+ README.md presentando el proyecto y el trabajo realizado

**`Cumplimiento de consigna`**:

+ Ejecución de consultas a la API
+ Opcional: cumplimiento de plus

## **Video demostrativo**

Tendrán que realizar un video presentando su trabajo y ejecutando las tareas solicitadas. **No deberá superar los 5 minutos**. Debe incluir una breve presentación personal, del proyecto, ejecución de código y tareas solicitadas.

Para presentarlo, deben entregar el link de acceso al video. Pueden alojarlo en YouTube, Drive o cualquier plataforma de almacenamiento. **Verificar que sea de acceso público**.

## **Checklist**

A modo de ayuda, dejamos algunos ítems para que puedan corroborar que su trabajo cumple con los requerimientos solicitados:

+ ¿Creaste funciones para tareas que puedan ser necesarias repetir en tu código?

+ ¿Comentaste tu código para que alguien más entienda qué hiciste o por qué, en cada bloque?

+ ¿Realizaste una limpieza adecuada de los datos?

+ ¿Levantaste un entorno containerized Docker con FastAPI?

+ ¿Se pueden realizar consultas a tu API?

+ ¿Tu repositorio tiene un README presentando el proyecto y el trabajo que hiciste para desarrollarlo?

+ ¿Los archivos, dentro de tu repositorio, están nombrados acorde a su contenido y organizados en carpetas?

+ ¿Hiciste un video demostrativo? ¿Incluye una presentación personal y del proyecto? ¿Hay una explicación del paso a paso?


## **Conceptos de interés**

- **`Docker`** es una solución completa para la producción, distribución y uso de containers.  
&nbsp;- **`Container`** es una abstracción de la capa de software que permite *empaquetar* código, con librerías y dependencias en un entorno parcialmente aislado.  
&nbsp;- **`Image`** es un ejecutable Docker que tiene todo lo necesario para correr aplicaciones, lo que incluye un archivo de configuración, variables -de entorno y runtime- y librerías.  
&nbsp;- **`Dockerfile`** archivo de texto con instrucciones para construir una imagen. Puede considerarse la automatización de creación de imágenes.  
- **`Deployment`** es el conjunto de actividades, infraestructura y recursos que posibilitan el uso de software. En este caso, la plataforma Mogenius les permitirá *montar* su imagen de Docker con la API en sus servidores para acceder a ella a través de internet.

## **Recursos y links provistos**

Imagen Docker con Uvicorn/Guinicorn para aplicaciones web de alta performance:

+ https://hub.docker.com/r/tiangolo/uvicorn-gunicorn-fastapi/ 

+ https://github.com/tiangolo/uvicorn-gunicorn-fastapi-docker

Mogenius Deployment

+ https://mogenius.com/home  

FAST API Documentation:

+ https://fastapi.tiangolo.com/tutorial/


## **Fuente de datos**

+ Podrán encontrar los archivos con datos en la carpeta Datasets, en este mismo repositorio.<sup>*</sup>

> <sub> <sup>*</sup>`Si quieren practicar un poco más, pueden investigar sobre la librería REQUESTS para descargar archivos desde links. Otra alternativa es usar el manejo nativo de Pandas para leer archivos directamente desde links`

## **Deadline**

Se les disponibilizará, a su debido tiempo, un form por el canal de Slack para que carguen sus links del repositorio de GitHub.  
La fecha límite para la entrega es el día **viernes 9 de diciembre a las 12:00 GMT-3**.   
  
<p align=center>
<img src = 'https://cdn.memegenerator.es/imagenes/memes/full/0/90/902781.jpg' height = 300></p>
