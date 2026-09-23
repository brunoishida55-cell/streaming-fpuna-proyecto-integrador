import json
import logging
from datetime import datetime, timezone, timedelta
from typing import Any

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms import trigger
from apache_beam.utils.timestamp import Duration

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

INPUT_TOPIC = "sensor.events.v1"
OUTPUT_TOPIC = "sensor.aggregates.v1"
BOOTSTRAP_SERVERS = "localhost:9092"
WINDOW_SECONDS = 60
ALLOWED_LATENESS_SECONDS = 30


REQUIRED_FIELDS = {"event_id", "key", "event_time", "payload"}
REQUIRED_PAYLOAD = {"sensor_id", "temperature_c", "humidity_pct"}


def parse_event(raw: bytes):
    """Deserializar JSON y separar eventos válidos de inválidos."""
    try:
        event = json.loads(raw.decode("utf-8"))
    except Exception as exc:
        logger.warning("JSON inválido: %s", exc)
        return

    missing = REQUIRED_FIELDS - event.keys()
    if missing:
        yield beam.pvalue.TaggedOutput("invalid", {"raw": str(raw), "reason": f"missing fields: {missing}"})
        return

    payload = event.get("payload", {})
    missing_payload = REQUIRED_PAYLOAD - payload.keys()
    if missing_payload:
        yield beam.pvalue.TaggedOutput("invalid", {"raw": str(raw), "reason": f"missing payload fields: {missing_payload}"})
        return

    yield event


class AssignEventTimestamp(beam.DoFn):
    """Asignar event_time del dominio como timestamp de Beam."""

    def process(self, event, *args, **kwargs):
        event_time_str = event["event_time"].replace("Z", "+00:00")
        event_time = datetime.fromisoformat(event_time_str)
        timestamp = event_time.timestamp()
        yield beam.window.TimestampedValue(event, timestamp)


class DeduplicateEvents(beam.DoFn):
    """Deduplicar por event_id dentro de cada clave (sensor)."""

    SEEN = beam.transforms.userstate.SetStateSpec(
        "seen", beam.coders.StrUtf8Coder()
    )

    def process(self, element, seen=beam.DoFn.StateParam(SEEN)):
        key, event = element
        event_id = event["event_id"]
        if event_id in seen.read():
            return
        seen.add(event_id)
        yield element


class ComputeAggregates(beam.DoFn):
    """Calcular promedio, máximo y conteo de temperatura por ventana."""

    def process(self, element, window=beam.DoFn.WindowParam):
        from datetime import UTC
        sensor_id, events = element
        events = list(events)

        temperatures = [e["payload"]["temperature_c"] for e in events]
        humidities = [e["payload"]["humidity_pct"] for e in events]

        start = datetime.fromtimestamp(window.start.seconds(), tz=UTC)
        end = datetime.fromtimestamp(window.end.seconds(), tz=UTC)

        result = {
            "sensor_id": sensor_id,
            "window_start": start.isoformat(),
            "window_end": end.isoformat(),
            "count": len(temperatures),
            "avg_temperature_c": round(sum(temperatures) / len(temperatures), 2),
            "max_temperature_c": max(temperatures),
            "avg_humidity_pct": round(sum(humidities) / len(humidities), 2),
            "schema_version": 1,
        }
        yield result


def run():
    options = PipelineOptions(
        streaming=True,
        runner="DirectRunner",
    )

    with beam.Pipeline(options=options) as p:
        raw_events = (
            p
            | "ReadFromKafka" >> beam.io.ReadFromKafka(
                consumer_config={"bootstrap.servers": BOOTSTRAP_SERVERS},
                topics=[INPUT_TOPIC],
                with_metadata=False,
            )
            | "ExtractValue" >> beam.Map(lambda kv: kv[1])
        )

        parsed = (
            raw_events
            | "ParseAndValidate" >> beam.ParDo(
                parse_event
            ).with_outputs("invalid", main="valid")
        )

        valid_events = parsed["valid"]
        invalid_events = parsed["invalid"]

        invalid_events | "LogInvalid" >> beam.Map(
            lambda e: logger.warning("Evento inválido: %s", e)
        )

        aggregates = (
            valid_events
            | "AssignTimestamp" >> beam.ParDo(AssignEventTimestamp())
            | "Window" >> beam.WindowInto(
                beam.window.FixedWindows(WINDOW_SECONDS),
                trigger=trigger.AfterWatermark(
                    early=trigger.AfterProcessingTime(10),
                    late=trigger.AfterCount(1),
                ),
                accumulation_mode=trigger.AccumulationMode.ACCUMULATING,
                allowed_lateness=Duration(seconds=ALLOWED_LATENESS_SECONDS),
            )
            | "KeyBySensor" >> beam.Map(lambda e: (e["payload"]["sensor_id"], e))
            | "Deduplicate" >> beam.ParDo(DeduplicateEvents())
            | "GroupBySensor" >> beam.GroupByKey()
            | "ComputeAggregates" >> beam.ParDo(ComputeAggregates())
        )

        (
            aggregates
            | "SerializeOutput" >> beam.Map(
                lambda r: (r["sensor_id"].encode("utf-8"), json.dumps(r).encode("utf-8"))
            )
            | "WriteToKafka" >> beam.io.WriteToKafka(
                producer_config={"bootstrap.servers": BOOTSTRAP_SERVERS},
                topic=OUTPUT_TOPIC,
            )
        )


if __name__ == "__main__":
    run()