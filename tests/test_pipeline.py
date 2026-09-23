import json
import unittest
from datetime import datetime, timezone

import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to
from apache_beam.transforms.window import FixedWindows

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from pipeline.pipeline import (
    parse_event,
    DeduplicateEvents,
    ComputeAggregates,
    WINDOW_SECONDS,
)


def make_event(sensor_id, temperature, event_time_str, event_id=None):
    return {
        "event_id": event_id or f"evt-{sensor_id}-{temperature}",
        "key": sensor_id,
        "event_time": event_time_str,
        "emitted_at": event_time_str,
        "schema_version": 1,
        "payload": {
            "sensor_id": sensor_id,
            "location": "laboratorio-A",
            "temperature_c": temperature,
            "humidity_pct": 50.0,
        },
    }


def encode(event):
    return json.dumps(event).encode("utf-8")


def to_kv_with_timestamp(event):
    t = datetime.fromisoformat(
        event["event_time"].replace("Z", "+00:00")
    ).timestamp()
    return beam.window.TimestampedValue(
        (event["payload"]["sensor_id"], event), t
    )


class TestParseEvent(unittest.TestCase):

    def test_valid_event_passes(self):
        event = make_event("sensor-01", 25.0, "2026-09-09T13:00:05Z")
        results = list(parse_event(encode(event)))
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["payload"]["temperature_c"], 25.0)

    def test_missing_field_is_invalid(self):
        bad = {"event_id": "x", "key": "sensor-01"}
        results = list(parse_event(json.dumps(bad).encode("utf-8")))
        self.assertEqual(len(results), 1)
        self.assertIsInstance(results[0], beam.pvalue.TaggedOutput)

    def test_invalid_json_is_skipped(self):
        results = list(parse_event(b"not-json"))
        self.assertEqual(len(results), 0)

    def test_invalid_event_is_tagged(self):
        bad = {"event_id": "x", "key": "sensor-01"}
        results = list(parse_event(json.dumps(bad).encode("utf-8")))
        self.assertEqual(len(results), 1)
        self.assertIsInstance(results[0], beam.pvalue.TaggedOutput)
        self.assertEqual(results[0].tag, "invalid")


class TestWindowsAndAggregates(unittest.TestCase):

    def test_duplicate_does_not_change_aggregate(self):
        event = make_event(
            "sensor-01", 25.0, "2026-09-09T13:00:05Z", event_id="dup-001"
        )

        with TestPipeline() as p:
            events = (
                p
                | beam.Create([event, event])
                | beam.Map(to_kv_with_timestamp)
                | beam.WindowInto(FixedWindows(WINDOW_SECONDS))
                | "Dedup" >> beam.ParDo(DeduplicateEvents())
                | beam.GroupByKey()
                | beam.ParDo(ComputeAggregates())
            )

            assert_that(
                events,
                equal_to([{
                    "sensor_id": "sensor-01",
                    "window_start": "2026-09-09T13:00:00+00:00",
                    "window_end": "2026-09-09T13:01:00+00:00",
                    "count": 1,
                    "avg_temperature_c": 25.0,
                    "max_temperature_c": 25.0,
                    "avg_humidity_pct": 50.0,
                    "schema_version": 1,
                }])
            )

    def test_out_of_order_event_uses_event_time_window(self):
        early = make_event(
            "sensor-02", 22.0, "2026-09-09T13:00:10Z", "early-001"
        )
        late = make_event(
            "sensor-02", 28.0, "2026-09-09T13:00:45Z", "late-001"
        )

        with TestPipeline() as p:
            events = (
                p
                | beam.Create([late, early])
                | beam.Map(to_kv_with_timestamp)
                | beam.WindowInto(FixedWindows(WINDOW_SECONDS))
                | "Dedup" >> beam.ParDo(DeduplicateEvents())
                | beam.GroupByKey()
                | beam.ParDo(ComputeAggregates())
            )

            assert_that(
                events,
                equal_to([{
                    "sensor_id": "sensor-02",
                    "window_start": "2026-09-09T13:00:00+00:00",
                    "window_end": "2026-09-09T13:01:00+00:00",
                    "count": 2,
                    "avg_temperature_c": 25.0,
                    "max_temperature_c": 28.0,
                    "avg_humidity_pct": 50.0,
                    "schema_version": 1,
                }])
            )


if __name__ == "__main__":
    unittest.main()