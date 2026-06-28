"""
stream_producer.py — PASSO 2
=============================
Legge i prezzi delle criptovalute dall'API pubblica di Binance e li PUBBLICA
(produce) sul topic Kafka `crypto-prices`, un messaggio JSON per simbolo.

Flusso:
    Binance REST API  ->  questo producer  ->  Kafka (topic: crypto-prices)

Concetti illustrati nel codice:
    - connessione a Kafka tramite bootstrap server
    - serializzazione del messaggio in JSON (bytes)
    - uso della KEY (= symbol) per l'ordinamento per partizione
    - loop di polling con intervallo configurabile
    - spegnimento "pulito" (Ctrl+C) con flush dei messaggi pendenti
"""

import json
import os
import signal
import sys
import time
from datetime import datetime, timezone

import requests
from dotenv import load_dotenv
from kafka import KafkaProducer
from kafka.errors import KafkaError

# --- Configurazione (letta da .env, con valori di default sensati) -----------
load_dotenv()  # carica le variabili dal file .env se presente

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "crypto-prices")
SYMBOLS = [s.strip().upper() for s in os.getenv("SYMBOLS", "BTCUSDT,ETHUSDT,SOLUSDT").split(",") if s.strip()]
POLL_INTERVAL = float(os.getenv("POLL_INTERVAL_SECONDS", "2"))

BINANCE_URL = "https://api.binance.com/api/v3/ticker/price"

# Flag per lo spegnimento pulito (lo impostiamo quando arriva Ctrl+C)
_running = True


def _handle_sigint(signum, frame):
    """Quando premi Ctrl+C non interrompiamo a metà: usciamo dal loop con calma."""
    global _running
    print("\n[producer] Ricevuto stop, chiudo dopo l'ultimo ciclo...")
    _running = False


def build_producer() -> KafkaProducer:
    """
    Crea il KafkaProducer.
    - value_serializer: trasforma il dict Python in JSON -> bytes (Kafka parla in bytes).
    - key_serializer: la chiave (symbol) in bytes; serve a decidere la partizione.
    - acks='all': aspettiamo conferma che il messaggio sia stato scritto (più affidabile).
    - retries: ritenta in automatico se c'è un errore transitorio.
    """
    return KafkaProducer(
        bootstrap_servers=BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        acks="all",
        retries=3,
        linger_ms=50,  # raggruppa i messaggi per ~50ms = più efficiente
    )


def fetch_prices(symbols: list[str]) -> list[dict]:
    """
    Chiama Binance e restituisce la lista di prezzi per i simboli richiesti.
    L'endpoint accetta il parametro `symbols` come array JSON nella query string.
    Risposta tipo: [{"symbol": "BTCUSDT", "price": "64950.12000000"}, ...]
    """
    # Binance vuole l'array JSON COMPATTO (senza spazi dopo le virgole),
    # altrimenti risponde 400 Bad Request.
    params = {"symbols": json.dumps(symbols, separators=(",", ":"))}
    resp = requests.get(BINANCE_URL, params=params, timeout=10)
    resp.raise_for_status()  # solleva eccezione se HTTP != 2xx
    return resp.json()


def to_message(raw: dict) -> dict:
    """Normalizza il dato grezzo di Binance nel nostro schema con timestamp."""
    now = datetime.now(timezone.utc)
    return {
        "symbol": raw["symbol"],
        "price": float(raw["price"]),
        "source": "binance",
        "event_time": now.isoformat(),          # ISO 8601, leggibile
        "ingest_ts": int(now.timestamp() * 1000) # epoch in millisecondi
    }


def main() -> None:
    signal.signal(signal.SIGINT, _handle_sigint)

    print(f"[producer] Bootstrap : {BOOTSTRAP}")
    print(f"[producer] Topic     : {TOPIC}")
    print(f"[producer] Simboli   : {', '.join(SYMBOLS)}")
    print(f"[producer] Intervallo: {POLL_INTERVAL}s")

    try:
        producer = build_producer()
    except KafkaError as e:
        print("[producer] ERRORE: nessun broker Kafka raggiungibile su "
              f"{BOOTSTRAP} ({e}). È attivo 'docker compose up -d'?")
        sys.exit(1)

    print("[producer] Connesso a Kafka. Inizio a pubblicare (Ctrl+C per fermare).\n")

    sent = 0
    while _running:
        cycle_start = time.time()
        try:
            for raw in fetch_prices(SYMBOLS):
                msg = to_message(raw)
                # key = symbol  ->  stesso simbolo, stessa partizione, ordine garantito
                producer.send(TOPIC, key=msg["symbol"], value=msg)
                sent += 1
                print(f"  -> {msg['symbol']:10s} {msg['price']:>15.4f}  @ {msg['event_time']}")
            producer.flush()  # forza l'invio effettivo a Kafka
            print(f"[producer] Ciclo ok. Messaggi totali inviati: {sent}\n")
        except requests.RequestException as e:
            print(f"[producer] Errore chiamando Binance (riprovo): {e}")

        # Aspetta il resto dell'intervallo (compensando il tempo già speso)
        elapsed = time.time() - cycle_start
        time.sleep(max(0.0, POLL_INTERVAL - elapsed))

    # Spegnimento pulito
    producer.flush()
    producer.close()
    print(f"[producer] Chiuso. Messaggi inviati in totale: {sent}")


if __name__ == "__main__":
    main()
