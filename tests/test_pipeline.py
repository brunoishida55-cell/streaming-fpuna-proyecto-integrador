import json
import os
import sys
import unittest
from datetime import datetime

import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to
from apache_beam.transforms.window import FixedWindows

sys.path.insert(
    0,
    os.path.join(os.path.dirname(__file__), ".."),
)

from pipeline.pipeline import (
    DeduplicateEvents,
    FormatAggregate,
    SensorStatsCombineFn,
    WINDOW_SECONDS,
    parse_event,
    serialize_output,
)


def make_event(
    sensor_id,
    temperature,
    event_time,
    event_id=None,
    humidity=50.0,
):
    return {
        "event_id": event_id or f"evt-{sensor_id}-{temperature}",
        "key": sensor_id,
        "event_time": event_time,
        "emitted_at": event_time,
        "schema_version": 1,
        "payload": {
            "sensor_id": sensor_id,
            "location": "laboratorio-A",
            "temperature_c": temperature,
            "humidity_pct": humidity,
        },
    }


def encode(event):
    return json.dumps(event).encode("utf-8")


def to_timestamped_kv(event):
    timestamp = datetime.fromisoformat(
        event["event_time"].replace("Z", "+00:00")
    ).timestamp()

    return beam.window.TimestampedValue(
        (
            event["payload"]["sensor_id"],
            event,
        ),
        timestamp,
    )


class TestParseEvent(unittest.TestCase):

    def test_valid_event_passes(self):
        event = make_event(
            "sensor-01",
            25.0,
            "2026-09-09T13:00:05Z",
        )

        results = list(parse_event(encode(event)))

        self.assertEqual(len(results), 1)
        self.assertEqual(
            results[0]["payload"]["temperature_c"],
            25.0,
        )

    def test_missing_field_is_invalid(self):
        event = {
            "event_id": "abc",
            "key": "sensor-01",
        }

        results = list(parse_event(encode(event)))

        self.assertEqual(len(results), 1)
        self.assertIsInstance(
            results[0],
            beam.pvalue.TaggedOutput,
        )
        self.assertEqual(results[0].tag, "invalid")

    def test_invalid_json_is_invalid(self):
        results = list(
            parse_event(b"esto-no-es-json")
        )

        self.assertEqual(len(results), 1)
        self.assertIsInstance(
            results[0],
            beam.pvalue.TaggedOutput,
        )
        self.assertEqual(results[0].tag, "invalid")

    def test_invalid_event_time_is_invalid(self):
        event = make_event(
            "sensor-01",
            25.0,
            "fecha-invalida",
        )

        results = list(parse_event(encode(event)))

        self.assertEqual(len(results), 1)
        self.assertIsInstance(
            results[0],
            beam.pvalue.TaggedOutput,
        )
        self.assertEqual(results[0].tag, "invalid")

    def test_key_must_match_sensor_id(self):
        event = make_event(
            "sensor-01",
            25.0,
            "2026-09-09T13:00:05Z",
        )

        event["key"] = "sensor-99"

        results = list(parse_event(encode(event)))

        self.assertEqual(len(results), 1)
        self.assertIsInstance(
            results[0],
            beam.pvalue.TaggedOutput,
        )
        self.assertEqual(results[0].tag, "invalid")


class TestWindowsDedupAndAggregates(unittest.TestCase):

    def test_duplicate_does_not_change_aggregate(self):
        event = make_event(
            "sensor-01",
            25.0,
            "2026-09-09T13:00:05Z",
            event_id="dup-001",
        )

        with TestPipeline() as pipeline:
            results = (
                pipeline
                | "CreateDuplicates"
                >> beam.Create([event, event])

                | "TimestampDuplicates"
                >> beam.Map(to_timestamped_kv)

                | "WindowDuplicates"
                >> beam.WindowInto(
                    FixedWindows(WINDOW_SECONDS)
                )

                | "DeduplicateDuplicates"
                >> beam.ParDo(DeduplicateEvents())

                | "CombineDuplicates"
                >> beam.CombinePerKey(
                    SensorStatsCombineFn()
                )

                | "FormatDuplicates"
                >> beam.ParDo(FormatAggregate())
            )

            assert_that(
                results,
                equal_to(
                    [
                        {
                            "sensor_id": "sensor-01",
                            "window_start":
                                "2026-09-09T13:00:00+00:00",
                            "window_end":
                                "2026-09-09T13:01:00+00:00",
                            "count": 1,
                            "avg_temperature_c": 25.0,
                            "max_temperature_c": 25.0,
                            "avg_humidity_pct": 50.0,
                            "schema_version": 1,
                        }
                    ]
                ),
            )

    def test_out_of_order_events_use_event_time(self):
        first_window = make_event(
            "sensor-02",
            22.0,
            "2026-09-09T13:00:10Z",
            event_id="evt-window-1",
        )

        second_window = make_event(
            "sensor-02",
            28.0,
            "2026-09-09T13:01:10Z",
            event_id="evt-window-2",
        )

        # Se entregan deliberadamente fuera de orden.
        events_out_of_order = [
            second_window,
            first_window,
        ]

        with TestPipeline() as pipeline:
            results = (
                pipeline
                | "CreateOutOfOrder"
                >> beam.Create(events_out_of_order)

                | "TimestampOutOfOrder"
                >> beam.Map(to_timestamped_kv)

                | "WindowOutOfOrder"
                >> beam.WindowInto(
                    FixedWindows(WINDOW_SECONDS)
                )

                | "DeduplicateOutOfOrder"
                >> beam.ParDo(DeduplicateEvents())

                | "CombineOutOfOrder"
                >> beam.CombinePerKey(
                    SensorStatsCombineFn()
                )

                | "FormatOutOfOrder"
                >> beam.ParDo(FormatAggregate())
            )

            assert_that(
                results,
                equal_to(
                    [
                        {
                            "sensor_id": "sensor-02",
                            "window_start":
                                "2026-09-09T13:00:00+00:00",
                            "window_end":
                                "2026-09-09T13:01:00+00:00",
                            "count": 1,
                            "avg_temperature_c": 22.0,
                            "max_temperature_c": 22.0,
                            "avg_humidity_pct": 50.0,
                            "schema_version": 1,
                        },
                        {
                            "sensor_id": "sensor-02",
                            "window_start":
                                "2026-09-09T13:01:00+00:00",
                            "window_end":
                                "2026-09-09T13:02:00+00:00",
                            "count": 1,
                            "avg_temperature_c": 28.0,
                            "max_temperature_c": 28.0,
                            "avg_humidity_pct": 50.0,
                            "schema_version": 1,
                        },
                    ]
                ),
            )

    def test_output_key_is_stable_by_sensor_and_window(self):
        aggregate = {
            "sensor_id": "sensor-03",
            "window_start":
                "2026-09-09T13:00:00+00:00",
            "window_end":
                "2026-09-09T13:01:00+00:00",
            "count": 3,
            "avg_temperature_c": 24.0,
            "max_temperature_c": 26.0,
            "avg_humidity_pct": 55.0,
            "schema_version": 1,
        }

        key, value = serialize_output(aggregate)

        self.assertEqual(
            key.decode("utf-8"),
            "sensor-03|2026-09-09T13:00:00+00:00",
        )

        self.assertEqual(
            json.loads(value.decode("utf-8")),
            aggregate,
        )


if __name__ == "__main__":
    unittest.main()
