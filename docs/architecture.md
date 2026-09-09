# Arquitectura del proyecto

## Caso de uso

El proyecto simula un sistema de monitoreo de temperatura y humedad mediante sensores instalados en diferentes laboratorios.

Los sensores generan mediciones continuamente. Los eventos son publicados en Apache Kafka y posteriormente serán procesados con Apache Beam para obtener métricas por sensor y por ventana temporal.

Las métricas principales serán:

- Temperatura promedio.
- Temperatura máxima.
- Cantidad de mediciones procesadas.

El productor permitirá simular también eventos duplicados y eventos tardíos o fuera de orden.

## Flujo general

Productor sintético -> Kafka -> Apache Beam -> Kafka de salida -> Consumidor

## Contrato del evento

Cada evento tendrá la siguiente estructura:

```json
{
  "event_id": "identificador-unico",
  "key": "sensor-01",
  "event_time": "2026-09-09T17:10:25.123Z",
  "emitted_at": "2026-09-09T17:10:26.020Z",
  "schema_version": 1,
  "payload": {
    "sensor_id": "sensor-01",
    "location": "laboratorio-A",
    "temperature_c": 28.7,
    "humidity_pct": 62.4
  }
}
```

## Significado de los campos

- `event_id`: identificador único y estable del evento. Se utilizará para detectar duplicados.
- `key`: clave de negocio y particionamiento. Utilizaremos el identificador del sensor.
- `event_time`: momento en que ocurrió realmente la medición.
- `emitted_at`: momento en que el productor publicó el evento.
- `schema_version`: versión del contrato del evento.
- `payload`: contiene los datos propios de la medición.

## Clave de Kafka

La clave será `sensor_id`.

Esto permite mantener relacionados los eventos correspondientes a un mismo sensor y posteriormente realizar agregaciones por sensor.

## Tópicos previstos

- `sensor.events.v1`: eventos originales producidos por los sensores.
- `sensor.aggregates.v1`: resultados procesados por Apache Beam.

## Integrantes

- Bruno Ishida
- Jessica Correa
- Julio Velotto
