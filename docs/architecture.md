# Arquitectura del proyecto

## 1. Caso de uso

El proyecto implementa un sistema de procesamiento de datos en streaming para el monitoreo de temperatura y humedad mediante sensores instalados en distintos laboratorios.

Tres sensores sintéticos generan mediciones y las publican en Apache Kafka. Apache Beam consume los eventos, valida su estructura, utiliza el tiempo del evento, elimina duplicados, agrupa por sensor dentro de ventanas temporales y calcula estadísticas.

Los resultados son publicados en un tópico Kafka de salida y pueden visualizarse mediante un consumidor incluido en el proyecto.

## 2. Flujo general

```text
Productor sintético
        |
        v
sensor.events.v1
   Apache Kafka
        |
        v
   Apache Beam
        |
        |-- Validación
        |-- Asignación de event_time
        |-- Ventanas fijas de 60 s
        |-- Deduplicación por event_id
        |-- Agregación incremental por sensor
        |
        v
sensor.aggregates.v1
   Apache Kafka
        |
        v
     Consumer
```

## 3. Contrato del evento de entrada

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

### Significado de los campos

- `event_id`: identificador único y estable del evento.
- `key`: clave de negocio utilizada para el particionamiento.
- `event_time`: instante en que ocurrió la medición.
- `emitted_at`: instante en que el productor emitió el evento.
- `schema_version`: versión del contrato.
- `payload.sensor_id`: identificador del sensor.
- `payload.location`: laboratorio asociado al sensor.
- `payload.temperature_c`: temperatura en grados Celsius.
- `payload.humidity_pct`: humedad relativa en porcentaje.

El pipeline verifica que `key` coincida con `payload.sensor_id`.

## 4. Tópicos Kafka

### Entrada

`sensor.events.v1`

Recibe los eventos originales producidos por los sensores.

### Salida

`sensor.aggregates.v1`

Recibe los agregados calculados por Apache Beam.

Ambos tópicos se crean con:

- 3 particiones.
- replication factor 1.

El tópico de salida utiliza además:

`cleanup.policy=compact`

## 5. Estrategia de particionamiento

La clave Kafka de entrada es `sensor_id`.

Esto permite que los eventos de un mismo sensor se asignen consistentemente a la misma partición y conserven su orden dentro de ella.

## 6. Procesamiento con Apache Beam

El pipeline aplica las siguientes etapas:

1. Lectura desde `sensor.events.v1`.
2. Deserialización del JSON.
3. Validación de campos obligatorios.
4. Separación de eventos válidos e inválidos.
5. Asignación de `event_time` como timestamp de Beam.
6. Ventanas fijas de 60 segundos.
7. Deduplicación mediante `event_id`.
8. Agregación incremental por sensor con `CombinePerKey`.
9. Cálculo de:
   - cantidad de mediciones;
   - temperatura promedio;
   - temperatura máxima;
   - humedad promedio.
10. Publicación en `sensor.aggregates.v1`.

## 7. Política de tiempo de evento

La semántica temporal se basa en `event_time`.

Se utilizan:

- ventanas fijas de 60 segundos;
- `allowed lateness` de 60 segundos;
- modo de acumulación `ACCUMULATING`;
- panes tempranos y tardíos activados por llegada de elementos.

El productor permite simular eventos tardíos modificando el valor de `event_time`.

## 8. Deduplicación

Los duplicados se detectan utilizando `event_id`.

El productor genera duplicados reenviando exactamente el mismo evento, por lo que conserva el mismo identificador.

El estado de deduplicación se mantiene por sensor y ventana. Por tanto, el horizonte práctico de deduplicación está acotado por la ventana y la lateness permitida.

## 9. Contrato de salida

Ejemplo:

```json
{
  "sensor_id": "sensor-01",
  "window_start": "2026-09-30T03:47:00+00:00",
  "window_end": "2026-09-30T03:48:00+00:00",
  "count": 2,
  "avg_temperature_c": 25.05,
  "max_temperature_c": 27.2,
  "avg_humidity_pct": 54.0,
  "schema_version": 1
}
```

## 10. Clave estable de salida e idempotencia

Cada resultado utiliza una clave con el formato:

`sensor_id|window_start`

Ejemplo:

`sensor-01|2026-09-30T03:47:00+00:00`

Los distintos panes de una misma combinación sensor-ventana utilizan la misma clave.

El tópico de salida está configurado con `cleanup.policy=compact`, por lo que esta estrategia permite una materialización de tipo latest-value/upsert.

La implementación no afirma ofrecer exactamente una vez de extremo a extremo.

## 11. Semántica de entrega

El productor Kafka utiliza confirmación de escritura y reintentos.

Apache Beam puede producir más de una actualización para una misma ventana debido a la política de panes acumulativos.

La combinación de:

- deduplicación por `event_id`;
- clave estable de salida;
- tópico compactado;
- productor Kafka idempotente del lado de Beam;

reduce el impacto de duplicados y permite interpretar los resultados mediante una estrategia de última versión.

## 12. Observabilidad

Durante la ejecución se registran:

- eventos inválidos;
- eventos duplicados descartados;
- agregados calculados por sensor y ventana;
- actividad de lectura y escritura en Kafka.

El consumidor incluido en el proyecto permite inspeccionar:

- clave Kafka;
- sensor;
- ventana;
- cantidad de mediciones;
- temperatura promedio;
- temperatura máxima;
- humedad promedio;
- partición y offset.

## 13. Pruebas

El proyecto incluye pruebas para:

- evento válido;
- campos faltantes;
- JSON inválido;
- `event_time` inválido;
- inconsistencia entre `key` y `sensor_id`;
- deduplicación;
- eventos fuera de orden;
- clave estable de salida.

La ejecución esperada es:

```text
Ran 8 tests
OK
```

## 14. Limitaciones

- Kafka utiliza replication factor 1.
- El pipeline se ejecuta localmente con `DirectRunner`.
- No existe un almacén externo de estado.
- La deduplicación tiene un horizonte limitado.
- Los panes acumulativos pueden producir múltiples actualizaciones para una misma ventana.
- No se garantiza exactamente una vez de extremo a extremo.
- La configuración está orientada a una demostración académica local.

## 15. Integrantes

- Bruno Ishida
- Jessica Correa
- Julio Velotto
