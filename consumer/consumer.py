import argparse
import json

from kafka import KafkaConsumer


BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "sensor.aggregates.v1"


def parse_args():
    parser = argparse.ArgumentParser(
        description="Consumidor de agregados del pipeline de sensores"
    )

    parser.add_argument(
        "--max-messages",
        type=int,
        default=0,
        help=(
            "Cantidad máxima de mensajes a consumir. "
            "0 significa ejecución continua."
        ),
    )

    return parser.parse_args()


def main():
    args = parse_args()

    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        group_id=None,
        key_deserializer=(
            lambda key: key.decode("utf-8")
            if key is not None
            else None
        ),
        value_deserializer=(
            lambda value: json.loads(
                value.decode("utf-8")
            )
        ),
    )

    print("=" * 70)
    print("Consumidor de resultados iniciado")
    print(f"Tópico: {TOPIC}")

    if args.max_messages > 0:
        print(
            f"Se leerán como máximo "
            f"{args.max_messages} mensajes."
        )
    else:
        print(
            "Ejecución continua. "
            "Presioná Ctrl+C para detener."
        )

    print("=" * 70)

    count = 0

    try:
        for message in consumer:
            result = message.value

            count += 1

            print()
            print("-" * 70)
            print(f"Mensaje #{count}")
            print(f"Kafka key              : {message.key}")
            print(f"Sensor                 : {result.get('sensor_id')}")
            print(
                f"Ventana                : "
                f"{result.get('window_start')} "
                f"-> {result.get('window_end')}"
            )
            print(
                f"Cantidad de mediciones : "
                f"{result.get('count')}"
            )
            print(
                f"Temperatura promedio   : "
                f"{result.get('avg_temperature_c')} °C"
            )
            print(
                f"Temperatura máxima     : "
                f"{result.get('max_temperature_c')} °C"
            )
            print(
                f"Humedad promedio       : "
                f"{result.get('avg_humidity_pct')} %"
            )
            print(
                f"Partición / offset     : "
                f"{message.partition} / {message.offset}"
            )

            if (
                args.max_messages > 0
                and count >= args.max_messages
            ):
                break

    except KeyboardInterrupt:
        print("\nConsumidor detenido por el usuario.")

    finally:
        consumer.close()
        print()
        print(
            f"Consumo finalizado. "
            f"Mensajes leídos: {count}"
        )


if __name__ == "__main__":
    main()
