# Proyecto Integrador - Data Streaming

Proyecto integrador de la asignatura **Streaming de datos y sus aplicaciones** de la Maestría en Inteligencia Artificial - FPUNA.

**Docente:** Rodrigo Parra, M.Sc.

## Integrantes

- Bruno Ishida
- Jessica Correa
- Julio Velotto

---

## 1. Caso de uso

El proyecto implementa un pipeline de procesamiento de datos en streaming para el monitoreo de temperatura y humedad mediante sensores ubicados en distintos laboratorios.

El sistema simula tres sensores que generan mediciones continuamente y publican eventos en Apache Kafka.

Apache Beam consume estos eventos, valida su estructura, utiliza el tiempo real del evento (`event_time`), elimina duplicados, agrupa las mediciones en ventanas temporales y calcula estadísticas por sensor.

Los resultados son publicados nuevamente en Kafka y pueden visualizarse mediante un consumidor incluido en el proyecto.

---

## 2. Arquitectura

```text
Productor sintetico
        |
        v
sensor.events.v1
   Apache Kafka
        |
        v
   Apache Beam
        |
        |-- Validacion
        |-- Event time
        |-- Ventanas de 60 segundos
        |-- Deduplicacion
        |-- Agregacion por sensor
        |
        v
sensor.aggregates.v1
   Apache Kafka
        |
        v
     Consumer
```

---

## 3. Tecnologias utilizadas

- Python 3.12
- Apache Kafka 4.3.1
- Apache Beam 2.76.0
- kafka-python 3.0.11
- Docker Desktop
- Docker Compose
- Java JDK 17

El pipeline se ejecuta localmente con `DirectRunner`.

---

## 4. Contrato del evento

```json
{
  "event_id": "identificador-unico",
  "key": "sensor-01",
  "event_time": "2026-09-30T03:47:23.101Z",
  "emitted_at": "2026-09-30T03:47:23.101Z",
  "schema_version": 1,
  "payload": {
    "sensor_id": "sensor-01",
    "location": "laboratorio-A",
    "temperature_c": 22.9,
    "humidity_pct": 45.6
  }
}
```

### Campos principales

- `event_id`: identificador unico y estable del evento.
- `key`: clave de negocio utilizada para identificar el sensor.
- `event_time`: momento en que ocurrio la medicion.
- `emitted_at`: momento en que el productor emitio el evento.
- `schema_version`: version del contrato.
- `payload`: datos propios de la medicion.

El pipeline verifica tambien que `key` coincida con `payload.sensor_id`.

---

## 5. Topicos Kafka

### Entrada

`sensor.events.v1`

Recibe los eventos originales generados por los sensores.

### Salida

`sensor.aggregates.v1`

Contiene los agregados calculados por Apache Beam.

Ambos topicos utilizan:

- 3 particiones.
- replication factor igual a 1.

El topico de salida se configura con:

`cleanup.policy=compact`

---

## 6. Estrategia de particionamiento

Los eventos utilizan como clave Kafka el identificador del sensor:

`sensor_id`

De esta manera, los eventos correspondientes al mismo sensor se asignan consistentemente a una misma particion y mantienen su orden dentro de ella.

---

## 7. Procesamiento con Apache Beam

El pipeline realiza:

1. Lectura desde `sensor.events.v1`.
2. Deserializacion JSON.
3. Validacion del contrato.
4. Separacion de eventos validos e invalidos.
5. Asignacion de `event_time` como timestamp de Beam.
6. Ventanas fijas de 60 segundos.
7. Deduplicacion mediante `event_id`.
8. Agregacion incremental por sensor con `CombinePerKey`.
9. Calculo de:
   - cantidad de mediciones;
   - temperatura promedio;
   - temperatura maxima;
   - humedad promedio.
10. Publicacion en `sensor.aggregates.v1`.

---

## 8. Politica temporal

El pipeline utiliza **event time** mediante el campo `event_time`.

Se utilizan:

- ventanas fijas de 60 segundos;
- `allowed lateness` de 60 segundos;
- acumulacion `ACCUMULATING`;
- panes tempranos y tardios activados por llegada de elementos.

El productor permite simular eventos tardios o fuera de orden.

---

## 9. Deduplicacion

La deduplicacion utiliza `event_id`.

Dos eventos con el mismo `event_id` se consideran el mismo evento logico.

El estado de deduplicacion se mantiene por sensor y ventana.

---

## 10. Salida e idempotencia

Cada agregado utiliza una clave estable:

`sensor_id|window_start`

Ejemplo:

`sensor-01|2026-09-30T03:47:00+00:00`

Los distintos panes correspondientes al mismo sensor y a la misma ventana utilizan la misma clave.

Esto permite una estrategia de tipo **upsert/latest value** sobre un topico compactado.

La implementacion no afirma garantizar exactamente una vez de extremo a extremo.

---

# Ejecucion

## 11. Prerrequisitos

Se requiere:

- Python 3.12
- Java JDK 17
- Docker Desktop
- Docker Compose

Comprobar:

```powershell
python --version
java -version
docker version
```

---

## 12. Crear entorno virtual

Desde la raiz del proyecto:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

---

## 13. Preparar Kafka automaticamente

Desde la raiz del proyecto:

```powershell
.\scripts\setup_kafka.ps1
```

El script:

- levanta Kafka con Docker Compose;
- espera hasta que Kafka este disponible;
- crea `sensor.events.v1`;
- crea `sensor.aggregates.v1`;
- configura `cleanup.policy=compact`;
- lista los topicos disponibles.

Al finalizar debe aparecer:

```text
Kafka preparado correctamente
```

### Alternativa manual

Si se desea crear los topicos manualmente:

```powershell
docker compose up -d
```

```powershell
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic sensor.events.v1 --partitions 3 --replication-factor 1
```

```powershell
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic sensor.aggregates.v1 --partitions 3 --replication-factor 1 --config cleanup.policy=compact
```

---

## 14. Ejecutar las pruebas

```powershell
python .	ests	est_pipeline.py
```

Resultado esperado:

```text
Ran 8 tests
OK
```

---

# Demo end-to-end

## 15. Generar eventos normales

```powershell
python .\producer\producer.py --max-events 3 --duplicate-rate 0 --late-rate 0
```

---

## 16. Ejecutar Apache Beam

```powershell
python .\pipeline\pipeline.py --max-records 3 --max-read-time 30
```

Ejemplo de salida:

```text
AGREGADO sensor=sensor-01 ... count=2 avg=25.05 max=27.2
AGREGADO sensor=sensor-03 ... count=1 avg=21.0 max=21.0
```

---

## 17. Consumir resultados

```powershell
python .\consumer\consumer.py --max-messages 4
```

Ejemplo:

```text
Kafka key              : sensor-01|2026-09-30T03:47:00+00:00
Sensor                 : sensor-01
Cantidad de mediciones : 2
Temperatura promedio   : 25.05 C
Temperatura maxima     : 27.2 C
Humedad promedio       : 54.0 %
```

---

## 18. Inspeccion directa del topico de salida

```powershell
docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic sensor.aggregates.v1 --from-beginning --timeout-ms 5000 --property print.key=true
```

El `TimeoutException` al finalizar es esperado si no aparecen mensajes nuevos dentro del intervalo indicado.

---

## 19. Demo de duplicados

```powershell
python .\producer\producer.py --max-events 3 --duplicate-rate 1 --late-rate 0
```

El evento duplicado conserva el mismo `event_id`, por lo que Beam puede descartarlo en la etapa de deduplicacion.

---

## 20. Demo de eventos tardios

```powershell
python .\producer\producer.py --max-events 3 --duplicate-rate 0 --late-rate 1 --late-seconds 30
```

La lateness permitida es de 60 segundos.

---

## 21. Detener el entorno

```powershell
docker compose stop
```

Para volver a iniciarlo:

```powershell
docker compose start
```

Para eliminar el entorno:

```powershell
docker compose down
```

---

# Estructura del repositorio

```text
.
├── consumer/
│   └── consumer.py
├── docs/
│   └── architecture.md
├── pipeline/
│   └── pipeline.py
├── producer/
│   └── producer.py
├── scripts/
│   └── setup_kafka.ps1
├── tests/
│   └── test_pipeline.py
├── docker-compose.yml
├── requirements.txt
└── README.md
```

---

## 22. Limitaciones

- Kafka utiliza replication factor 1.
- El pipeline se ejecuta con `DirectRunner`.
- No se implementa un almacen externo de estado.
- No se garantiza exactamente una vez de extremo a extremo.
- La deduplicacion tiene un horizonte limitado.
- Los panes acumulativos pueden producir mas de una actualizacion para una misma combinacion sensor-ventana.
- La clave estable y el topico compactado permiten una estrategia de ultima version.

---

## 23. Resultado esperado

```text
Fuente
  -> Kafka de entrada
  -> Apache Beam
  -> Kafka de salida
  -> Consumer
```

La demo permite observar eventos normales, duplicados y tardios.
