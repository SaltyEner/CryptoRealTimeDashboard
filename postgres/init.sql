-- =============================================================================
-- init.sql — PASSO 3
-- Eseguito automaticamente da PostgreSQL al PRIMO avvio del container
-- (i file in /docker-entrypoint-initdb.d/ vengono lanciati una volta sola,
--  quando il volume dati è vuoto).
-- =============================================================================

-- Tabella 1: ogni "tick" di prezzo ripulito (append-only).
-- È il dato grezzo-ma-tipizzato che arriva dallo stream.
CREATE TABLE IF NOT EXISTS crypto_prices (
    id           BIGSERIAL PRIMARY KEY,
    symbol       TEXT             NOT NULL,
    price        DOUBLE PRECISION NOT NULL,
    source       TEXT,
    event_time   TIMESTAMPTZ      NOT NULL,
    ingest_ts    BIGINT,
    processed_at TIMESTAMPTZ      DEFAULT now()
);

-- Indice per query rapide "ultimi prezzi di un simbolo"
CREATE INDEX IF NOT EXISTS idx_crypto_prices_symbol_time
    ON crypto_prices (symbol, event_time DESC);

-- Tabella 2: aggregazione a finestra di 1 minuto (media/min/max/conteggio).
-- La PK (symbol, window_start) evita righe duplicate per la stessa finestra.
CREATE TABLE IF NOT EXISTS crypto_agg_1min (
    symbol       TEXT             NOT NULL,
    window_start TIMESTAMPTZ      NOT NULL,
    window_end   TIMESTAMPTZ      NOT NULL,
    avg_price    DOUBLE PRECISION,
    min_price    DOUBLE PRECISION,
    max_price    DOUBLE PRECISION,
    n_ticks      BIGINT,
    PRIMARY KEY (symbol, window_start)
);
