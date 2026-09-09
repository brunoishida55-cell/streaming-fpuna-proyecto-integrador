import argparse
import json
import random
import time
import uuid
from datetime import datetime, timedelta, timezone

from kafka import KafkaProducer


BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "sensor.events.v1"

SENSORS = [
    {"sensor_id": "sensor-01", "location": "laboratorio-A"},
    {"sensor_id": "sensor-02", "location": "laboratorio-B"},
    {"sensor_id": "sensor-03", "location": "laboratorio-C"},
]


def utc_iso(dt):
    return (
        dt.astimezone(timezone.utc)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z")
    )


def create_event(sensor, event_number, seed, late=False, late_seconds=30):
    now = datetime.now(timezone.utc)

    if late:
        event_time = now - timedelta(seconds=late_seconds)
    else:
        event_time = now

    event_id = str(
        uuid.uuid5(
            uuid.NAMESPACE_DNS,
            f"sensor-event-{seed}-{event_number}"
        )
    )

    return {
        "event_id": event_id,
        "key": sensor["sensor_id"],
        "event_time": utc_iso(event_time),
        "emitted_at": utc_iso(now),
        "schema_version": 1,
        "payload": {
            "sensor_id": sensor["sensor_id"],
            "location": sensor["location"],
            "temperature_c": round(random.uniform(20.0, 32.0), 1),
            "humidity_pct": round(random.uniform(40.0, 80.0), 1),
        },
    }


def send_event(producer, event, event_type):
    metadata = producer.send(
        TOPIC,
        key=event["key"],
        value=event,
    ).get(timeout=10)

    payload = event["payload"]

    print(
        f"{event_type:<10} | "
        f"{event['key']} | "
        f"T={payload['temperature_c']} C | "
        f"H={payload['humidity_pct']}% | "
        f"event_time={event['event_time']} | "
        f"partition={metadata.partition} | "
        f"offset={metadata.offset} | "
        f"id={event['event_id'][:8]}"
    )


def parse_args():
    parser = argparse.ArgumentParser(
        description="Productor sintetico de sensores para Kafka"
    )

    parser.add_argument(
        "--interval",
        type=float,
        default=1.0,
        help="Segundos entre mediciones. Default: 1",
    )

    parser.add_argument(
        "--duplicate-rate",
        type=float,
        default=0.10,
        help="Probabilidad de duplicar un evento. Default: 0.10",
    )

    parser.add_argument(
        "--late-rate",
        type=float,
        default=0.10,
        help="Probabilidad de generar un evento tardio. Default: 0.10",
    )

    parser.add_argument(
        "--late-seconds",
        type=int,
        default=30,
        help="Antiguedad del event_time de eventos tardios. Default: 30",
    )

    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Semilla para generacion reproducible. Default: 42",
    )

    parser.add_argument(
        "--max-events",
        type=int,
        default=0,
        help="Cantidad de eventos base. 0 significa ejecucion continua",
    )

    return parser.parse_args()


def main():
    args = parse_args()

    if not 0 <= args.duplicate_rate <= 1:
        raise ValueError("--duplicate-rate debe estar entre 0 y 1")

    if not 0 <= args.late_rate <= 1:
        raise ValueError("--late-rate debe estar entre 0 y 1")

    random.seed(args.seed)

    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        key_serializer=lambda key: key.encode("utf-8"),
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        acks="all",
        retries=5,
    )

    print(f"Productor iniciado. Topico: {TOPIC}")
    print(f"Seed: {args.seed}")
    print(f"Duplicate rate: {args.duplicate_rate}")
    print(f"Late rate: {args.late_rate}")
    print(f"Late seconds: {args.late_seconds}")
    print("Presiona Ctrl+C para detenerlo.")
    print()

    event_number = 1

    try:
        while True:
            if args.max_events > 0 and event_number > args.max_events:
                break

            sensor = random.choice(SENSORS)

            is_late = random.random() < args.late_rate

            event = create_event(
                sensor=sensor,
                event_number=event_number,
                seed=args.seed,
                late=is_late,
                late_seconds=args.late_seconds,
            )

            if is_late:
                send_event(producer, event, "LATE")
            else:
                send_event(producer, event, "NORMAL")

            if random.random() < args.duplicate_rate:
                send_event(producer, event, "DUPLICATE")

            event_number += 1
            time.sleep(args.interval)

    except KeyboardInterrupt:
        print("\nProductor detenido por el usuario.")

    finally:
        producer.flush()
        producer.close()
        print("Conexion con Kafka cerrada.")


if __name__ == "__main__":
    main()