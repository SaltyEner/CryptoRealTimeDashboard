# =============================================================================
# Dockerfile del producer (Passo 7) — rende il producer un servizio Docker,
# così "docker compose up -d" avvia ANCHE la sorgente dati (vero one-command).
# =============================================================================
FROM python:3.12-slim

WORKDIR /app

# Prima le dipendenze (sfrutta la cache: si reinstalla solo se cambia il file).
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Poi il codice.
COPY stream_producer.py .

# -u = output non bufferizzato: i print compaiono subito in "docker logs".
CMD ["python", "-u", "stream_producer.py"]
