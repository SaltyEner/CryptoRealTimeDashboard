"""
crypto_pipeline_health.py — PASSO 4
===================================
DAG di Airflow che monitora la salute della pipeline:
    1) check_kafka          -> Kafka raggiungibile sulla porta interna?
    2) check_postgres       -> il DB della pipeline risponde?
    3) check_data_freshness -> sono arrivate righe negli ultimi 5 minuti?
                               (se NO, la pipeline è ferma -> task fallisce -> alert)

Dipendenze:  [check_kafka, check_postgres]  ->  check_data_freshness

Gira dentro la rete Docker, quindi raggiunge gli altri servizi per NOME
(kafka:29092, postgres:5432).
"""

from __future__ import annotations

import socket
from datetime import timedelta

import pendulum
from airflow.decorators import dag, task


def alert_callback(context) -> None:
    """'Alerting' minimale: in un progetto reale qui invieresti email/Slack."""
    ti = context.get("task_instance")
    print(f"[ALERT] Task '{ti.task_id}' del DAG '{ti.dag_id}' è FALLITO! "
          f"Controlla la pipeline.")


DEFAULT_ARGS = {
    "owner": "data-eng",
    "retries": 2,                          # ritenta 2 volte prima di arrendersi
    "retry_delay": timedelta(seconds=30),
    "on_failure_callback": alert_callback,  # alert quando un task fallisce
}

# Parametri di connessione (nomi dei servizi nella rete Docker)
PG = dict(host="postgres", dbname="crypto", user="pipeline", password="pipeline")
KAFKA_HOST, KAFKA_PORT = "kafka", 29092


@dag(
    dag_id="crypto_pipeline_health",
    description="Health check della pipeline crypto (Kafka, Postgres, freschezza dati)",
    schedule="*/5 * * * *",                 # ogni 5 minuti
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,                          # non recuperare run passati
    default_args=DEFAULT_ARGS,
    tags=["crypto", "health"],
)
def crypto_pipeline_health():

    @task
    def check_kafka() -> str:
        """Verifica che la porta del broker Kafka sia aperta (test TCP)."""
        with socket.create_connection((KAFKA_HOST, KAFKA_PORT), timeout=10):
            pass
        msg = f"Kafka OK ({KAFKA_HOST}:{KAFKA_PORT})"
        print(msg)
        return msg

    @task
    def check_postgres() -> str:
        """Verifica che il DB della pipeline risponda (SELECT 1)."""
        import psycopg2
        conn = psycopg2.connect(**PG)
        try:
            cur = conn.cursor()
            cur.execute("SELECT 1;")
            cur.fetchone()
        finally:
            conn.close()
        print("Postgres OK")
        return "Postgres OK"

    @task
    def check_data_freshness() -> int:
        """
        Conta le righe arrivate negli ultimi 5 minuti.
        Se 0 -> la pipeline non sta ricevendo dati -> solleviamo errore (alert).
        """
        import psycopg2
        conn = psycopg2.connect(**PG)
        try:
            cur = conn.cursor()
            cur.execute(
                "SELECT count(*) FROM crypto_prices "
                "WHERE event_time > now() - interval '5 minutes';"
            )
            n = cur.fetchone()[0]
        finally:
            conn.close()
        print(f"Righe negli ultimi 5 minuti: {n}")
        if n == 0:
            raise ValueError("PIPELINE FERMA: nessun dato negli ultimi 5 minuti!")
        return n

    # Dipendenze: i due check infrastrutturali prima, poi la freschezza dati
    [check_kafka(), check_postgres()] >> check_data_freshness()


crypto_pipeline_health()
