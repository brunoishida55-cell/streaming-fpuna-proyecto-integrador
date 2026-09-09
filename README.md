# Proyecto Integrador - Data Streaming



Proyecto integrador de la asignatura **Streaming de datos y sus aplicaciones** de la Maestría en Inteligencia Artificial - FPUNA.



**Docente:** Rodrigo Parra, M.Sc.



## Caso de uso



El proyecto implementa un pipeline de procesamiento de datos en streaming para el monitoreo de temperatura y humedad mediante sensores ubicados en distintos laboratorios.



La arquitectura general prevista es:



Fuente de eventos -> Apache Kafka -> Apache Beam -> Salida



El productor sintÃ©tico genera mediciones de tres sensores y permite simular eventos normales, duplicados y eventos tardÃ­os o fuera de orden.



## Arquitectura



### Topico de entrada



`sensor.events.v1`



Recibe los eventos generados por los sensores.



### Topico de salida



`sensor.aggregates.v1`



SerÃ¡ utilizado posteriormente por Apache Beam para publicar los resultados procesados.



### Particiones



Los dos topicos utilizan:



\- 3 particiones

\- replication factor igual a 1



La clave Kafka utilizada es `sensor\_id`.



Esto permite que los eventos correspondientes al mismo sensor sean enviados consistentemente a una particion y conserven su orden dentro de ella.



## Prerrequisitos



El proyecto fue probado utilizando:



\- Docker 29.3.1

\- Docker Compose v5.1.1

\- Python 3.12.10

\- Git

\- PowerShell en Windows



## 1. Levantar Apache Kafka



Desde la carpeta raiz del proyecto ejecutar:



```powershell

docker compose up -d
