import argparse
import json
import logging
from datetime import datetime, timezone

import apache_beam as beam
from apache_beam.io.kafka import ReadFromKafka, WriteToKafka
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms import trigger
from apache_beam.utils.timestamp import Duration


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


INPUT_TOPIC = "sensor.events.v1"
OUTPUT_TOPIC = "sensor.aggregates.v1"
BOOTSTRAP_SERVERS = "localhost:9092"

WINDOW_SECONDS = 60

# El productor simula eventos con 30 s de retraso.
# Permitimos hasta 60 s para dar margen al procesamiento.
ALLOWED_LATENESS_SECONDS = 60


REQUIRED_FIELDS = {
    "event_id",
    "key",
    "event_time",
    "emitted_at",
    "schema_version",
    "payload",
}

REQUIRED_PAYLOAD_FIELDS = {
    "sensor_id",
    "location",
    "temperature_c",
    "humidity_pct",
}


def parse_datetime(value):
    """Convertir un timestamp ISO-8601 a datetime con zona horaria."""
    if not isinstance(value, str):
        raise ValueError("timestamp must be a string")

    dt = datetime.fromisoformat(value.replace("Z", "+00:00"))

    if dt.tzinfo is None:
        raise ValueError("timestamp must include timezone")

    return dt


def invalid_output(raw, reason):
    """Crear una salida lateral para eventos inválidos."""
    return beam.pvalue.TaggedOutput(
        "invalid",
        {
            "raw": repr(raw),
            "reason": reason,
        },
    )


def parse_event(raw):
    """Deserializar y validar el contrato del evento."""

    if raw is None:
        yield invalid_output(raw, "Kafka value is null")
        return

    try:
        event = json.loads(raw.decode("utf-8"))
    except Exception as exc:
        yield invalid_output(raw, f"invalid JSON: {exc}")
        return

    if not isinstance(event, dict):
        yield invalid_output(raw, "event must be a JSON object")
        return

    missing = REQUIRED_FIELDS - event.keys()

    if missing:
        yield invalid_output(
            raw,
            f"missing fields: {sorted(missing)}",
        )
        return

    payload = event.get("payload")

    if not isinstance(payload, dict):
        yield invalid_output(raw, "payload must be an object")
        return

    missing_payload = REQUIRED_PAYLOAD_FIELDS - payload.keys()

    if missing_payload:
        yield invalid_output(
            raw,
            f"missing payload fields: {sorted(missing_payload)}",
        )
        return

    try:
        parse_datetime(event["event_time"])
        parse_datetime(event["emitted_at"])

        if not isinstance(event["event_id"], str) or not event["event_id"]:
            raise ValueError("event_id must be a non-empty string")

        if not isinstance(event["key"], str) or not event["key"]:
            raise ValueError("key must be a non-empty string")

        if event["key"] != payload["sensor_id"]:
            raise ValueError("key must match payload.sensor_id")

        if event["schema_version"] != 1:
            raise ValueError("unsupported schema_version")

        float(payload["temperature_c"])
        float(payload["humidity_pct"])

    except Exception as exc:
        yield invalid_output(raw, str(exc))
        return

    yield event


class AssignEventTimestamp(beam.DoFn):
    """Usar event_time como timestamp de evento de Apache Beam."""

    def process(self, event):
        event_time = parse_datetime(event["event_time"])

        yield beam.window.TimestampedValue(
            event,
            event_time.timestamp(),
        )


class DeduplicateEvents(beam.DoFn):
    """
    Deduplicar por event_id.

    Como este DoFn se ejecuta después de WindowInto, el estado queda
    asociado a sensor y ventana. Por tanto, el horizonte de deduplicación
    corresponde a la ventana más la lateness permitida.
    """

    SEEN = beam.transforms.userstate.SetStateSpec(
        "seen_event_ids",
        beam.coders.StrUtf8Coder(),
    )

    def process(
        self,
        element,
        seen=beam.DoFn.StateParam(SEEN),
    ):
        sensor_id, event = element
        event_id = event["event_id"]

        if event_id in set(seen.read()):
            logger.info(
                "Duplicado descartado: sensor=%s event_id=%s",
                sensor_id,
                event_id,
            )
            return

        seen.add(event_id)
        yield element


class SensorStatsCombineFn(beam.CombineFn):
    """Agregación incremental de las mediciones por sensor."""

    def create_accumulator(self):
        return {
            "count": 0,
            "temperature_sum": 0.0,
            "temperature_max": None,
            "humidity_sum": 0.0,
        }

    def add_input(self, accumulator, event):
        temperature = float(event["payload"]["temperature_c"])
        humidity = float(event["payload"]["humidity_pct"])

        accumulator["count"] += 1
        accumulator["temperature_sum"] += temperature
        accumulator["humidity_sum"] += humidity

        if accumulator["temperature_max"] is None:
            accumulator["temperature_max"] = temperature
        else:
            accumulator["temperature_max"] = max(
                accumulator["temperature_max"],
                temperature,
            )

        return accumulator

    def merge_accumulators(self, accumulators):
        merged = self.create_accumulator()

        for accumulator in accumulators:
            merged["count"] += accumulator["count"]
            merged["temperature_sum"] += accumulator["temperature_sum"]
            merged["humidity_sum"] += accumulator["humidity_sum"]

            current_max = accumulator["temperature_max"]

            if current_max is not None:
                if merged["temperature_max"] is None:
                    merged["temperature_max"] = current_max
                else:
                    merged["temperature_max"] = max(
                        merged["temperature_max"],
                        current_max,
                    )

        return merged

    def extract_output(self, accumulator):
        count = accumulator["count"]

        if count == 0:
            return {
                "count": 0,
                "avg_temperature_c": None,
                "max_temperature_c": None,
                "avg_humidity_pct": None,
            }

        return {
            "count": count,
            "avg_temperature_c": round(
                accumulator["temperature_sum"] / count,
                2,
            ),
            "max_temperature_c": accumulator["temperature_max"],
            "avg_humidity_pct": round(
                accumulator["humidity_sum"] / count,
                2,
            ),
        }


class FormatAggregate(beam.DoFn):
    """Agregar información de ventana al resultado."""

    def process(
        self,
        element,
        window=beam.DoFn.WindowParam,
    ):
        sensor_id, stats = element

        start = datetime.fromtimestamp(
            float(window.start),
            tz=timezone.utc,
        )

        end = datetime.fromtimestamp(
            float(window.end),
            tz=timezone.utc,
        )

        result = {
            "sensor_id": sensor_id,
            "window_start": start.isoformat(),
            "window_end": end.isoformat(),
            "count": stats["count"],
            "avg_temperature_c": stats["avg_temperature_c"],
            "max_temperature_c": stats["max_temperature_c"],
            "avg_humidity_pct": stats["avg_humidity_pct"],
            "schema_version": 1,
        }

        yield result


def log_aggregate(result):
    logger.info(
        "AGREGADO sensor=%s ventana=%s count=%s avg=%s max=%s",
        result["sensor_id"],
        result["window_start"],
        result["count"],
        result["avg_temperature_c"],
        result["max_temperature_c"],
    )

    return result


def serialize_output(result):
    """
    Utilizar una clave estable por sensor y ventana.

    Esto permite que diferentes panes de una misma ventana tengan
    la misma identidad lógica para una eventual materialización/upsert.
    """

    kafka_key = (
        f'{result["sensor_id"]}|{result["window_start"]}'
    ).encode("utf-8")

    kafka_value = json.dumps(result).encode("utf-8")

    return kafka_key, kafka_value


def parse_args():
    parser = argparse.ArgumentParser(
        description="Pipeline Apache Beam para sensores"
    )

    parser.add_argument(
        "--max-records",
        type=int,
        default=0,
        help=(
            "Cantidad máxima de registros Kafka a leer. "
            "0 significa ejecución continua."
        ),
    )

    parser.add_argument(
        "--max-read-time",
        type=int,
        default=0,
        help=(
            "Tiempo máximo de lectura desde Kafka en segundos. "
            "0 significa sin límite."
        ),
    )

    return parser.parse_args()


def run():
    args = parse_args()

    max_records = (
        args.max_records
        if args.max_records > 0
        else None
    )

    max_read_time = (
        args.max_read_time
        if args.max_read_time > 0
        else None
    )

    options = PipelineOptions(
        flags=[],
        streaming=True,
        runner="DirectRunner",
    )

    with beam.Pipeline(options=options) as pipeline:

        raw_events = (
            pipeline
            | "ReadFromKafka"
            >> ReadFromKafka(
                consumer_config={
                    "bootstrap.servers": BOOTSTRAP_SERVERS,
                    "auto.offset.reset": "earliest",
                    "enable.auto.commit": "false",
                    "group.id": "beam-sensor-pipeline",
                },
                topics=[INPUT_TOPIC],
                with_metadata=False,
                max_num_records=max_records,
                max_read_time=max_read_time,
            )
            | "ExtractValue"
            >> beam.Map(lambda kv: kv[1])
        )

        parsed = (
            raw_events
            | "ParseAndValidate"
            >> beam.ParDo(parse_event).with_outputs(
                "invalid",
                main="valid",
            )
        )

        valid_events = parsed["valid"]
        invalid_events = parsed["invalid"]

        invalid_events | "LogInvalid" >> beam.Map(
            lambda event: logger.warning(
                "Evento inválido: %s",
                event,
            )
        )

        aggregates = (
            valid_events
            | "AssignEventTimestamp"
            >> beam.ParDo(AssignEventTimestamp())

            | "FixedWindow60s"
            >> beam.WindowInto(
                beam.window.FixedWindows(WINDOW_SECONDS),
                trigger=trigger.AfterWatermark(
                    early=trigger.AfterCount(1),
                    late=trigger.AfterCount(1),
                ),
                accumulation_mode=(
                    trigger.AccumulationMode.ACCUMULATING
                ),
                allowed_lateness=Duration(
                    seconds=ALLOWED_LATENESS_SECONDS
                ),
            )

            | "KeyBySensor"
            >> beam.Map(
                lambda event: (
                    event["payload"]["sensor_id"],
                    event,
                )
            ).with_output_types(
                beam.typehints.KV[str, dict]
            )


            | "DeduplicateByEventId"
            >> beam.ParDo(DeduplicateEvents())

            | "CombineBySensor"
            >> beam.CombinePerKey(
                SensorStatsCombineFn()
            )

            | "FormatAggregate"
            >> beam.ParDo(FormatAggregate())

            | "LogAggregate"
            >> beam.Map(log_aggregate)
        )

        (
            aggregates
            | "SerializeOutput"
            >> beam.Map(serialize_output).with_output_types(
                beam.typehints.KV[bytes, bytes]
            )

            | "WriteToKafka"
            >> WriteToKafka(
                producer_config={
                    "bootstrap.servers": BOOTSTRAP_SERVERS,
                },
                topic=OUTPUT_TOPIC,
            )
        )


if __name__ == "__main__":
    run()
    run()
