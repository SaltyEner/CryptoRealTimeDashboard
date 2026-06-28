"""
test_consumer.py — PASSO 2 (verifica)
=====================================
Un piccolo CONSUMER di test: si collega al topic `crypto-prices`, legge i
messaggi che il producer sta pubblicando e li stampa a schermo.

Serve SOLO a verificare che il flusso producer -> Kafka funzioni.
Nel Passo 3 il "consumer vero" sarà Spark Structured Streaming.

Avvio tipico (in un secondo terminale, mentre gira il producer):
    python test_consumer.py
"""

import json
import os

from dotenv import load_dotenv
from kafka import KafkaConsumer

load_dotenv()

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "crypto-prices")


def main() -> None:
    print(f"[consumer] In ascolto su topic '{TOPIC}' @ {BOOTSTRAP} (Ctrl+C per fermare)\n")

    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=BOOTSTRAP,
        # auto_offset_reset='latest': leggi solo i NUOVI messaggi da ora in poi.
        # (usa 'earliest' se vuoi rileggere tutto lo storico del topic)
        auto_offset_reset="latest",
        group_id="test-consumer-group",
        # Deserializza i bytes JSON in dict Python
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        key_deserializer=lambda k: k.decode("utf-8") if k else None,
    )

    count = 0
    try:
        for record in consumer:
            count += 1
            # Mostriamo anche partizione e offset: ecco i concetti "dal vivo"!
            print(f"[p{record.partition} @ offset {record.offset}] "
                  f"key={record.key:10s} -> {record.value}")
    except KeyboardInterrupt:
        print(f"\n[consumer] Fermato. Messaggi letti: {count}")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
