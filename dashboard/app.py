"""
app.py — PASSO 6 (rev. grafica: grafici Plotly)
===============================================
Dashboard Streamlit "live" sui dati della pipeline, letti da PostgreSQL.

Mostra:
    - metriche: prezzo corrente per simbolo (+ variazione nella finestra)
    - grafico AREA (Plotly): andamento prezzo del simbolo selezionato, colore
      verde/rosso secondo il trend, con tooltip al passaggio del mouse
    - grafico CANDLESTICK (Plotly): candele costruite dagli aggregati 1-min
    - tabella: aggregazione a 1 minuto da crypto_agg_1min

Streamlit riesegue tutto lo script ad ogni rerun; usiamo il caching per la
connessione (st.cache_resource) e per le query (st.cache_data con ttl breve).
"""

import os

import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from sqlalchemy import create_engine, text
from streamlit_autorefresh import st_autorefresh

# --- Palette coordinata col tema (vedi .streamlit/config.toml) ---------------
ACCENT = "#00d09c"   # verde acqua di brand
GREEN = "#26a69a"    # rialzo
RED = "#ef5350"      # ribasso
GRID = "rgba(255,255,255,0.06)"

# Etichette "carine" per i simboli noti (fallback = simbolo grezzo).
COIN_LABEL = {"BTCUSDT": "₿ BTC", "ETHUSDT": "Ξ ETH", "SOLUSDT": "◎ SOL"}

# --- Connessione (parametri da env, default = servizio Docker 'postgres') ----
PGHOST = os.getenv("PGHOST", "postgres")
PGPORT = os.getenv("PGPORT", "5432")
PGDATABASE = os.getenv("PGDATABASE", "crypto")
PGUSER = os.getenv("PGUSER", "pipeline")
PGPASSWORD = os.getenv("PGPASSWORD", "pipeline")
DB_URL = f"postgresql+psycopg2://{PGUSER}:{PGPASSWORD}@{PGHOST}:{PGPORT}/{PGDATABASE}"


@st.cache_resource
def get_engine():
    """La connessione/engine è costosa: la creiamo una volta sola (cache)."""
    return create_engine(DB_URL, pool_pre_ping=True)


@st.cache_data(ttl=5)  # i risultati durano 5s: dashboard "live" ma senza martellare il DB
def query_df(sql: str, params: dict | None = None) -> pd.DataFrame:
    with get_engine().connect() as conn:
        return pd.read_sql(text(sql), conn, params=params or {})


def style_fig(fig: go.Figure, height: int = 340) -> go.Figure:
    """Applica lo stile scuro coerente col tema a un grafico Plotly."""
    fig.update_layout(
        template="plotly_dark",
        height=height,
        margin=dict(l=10, r=10, t=10, b=10),
        paper_bgcolor="rgba(0,0,0,0)",   # trasparente -> eredita lo sfondo della pagina
        plot_bgcolor="rgba(0,0,0,0)",
        showlegend=False,
        hovermode="x unified",
        xaxis=dict(showgrid=False),
        yaxis=dict(gridcolor=GRID, zeroline=False),
    )
    return fig


# --- Layout ------------------------------------------------------------------
st.set_page_config(page_title="Crypto Real-Time Pipeline", page_icon="📈", layout="wide")

# Header con badge "LIVE" pulsante (CSS) per dare l'idea del flusso in tempo reale.
st.markdown(
    """
    <style>
      .live-badge{display:inline-block;padding:2px 10px;border-radius:999px;
        background:rgba(0,208,156,.15);color:#00d09c;font-weight:700;font-size:.8rem;
        border:1px solid rgba(0,208,156,.4);vertical-align:middle;margin-left:.5rem;}
      .live-dot{height:8px;width:8px;background:#00d09c;border-radius:50%;
        display:inline-block;margin-right:6px;animation:pulse 1.4s infinite;}
      @keyframes pulse{0%{opacity:1}50%{opacity:.25}100%{opacity:1}}
    </style>
    """,
    unsafe_allow_html=True,
)
st.markdown(
    "# 📈 Crypto Real-Time Dashboard "
    '<span class="live-badge"><span class="live-dot"></span>LIVE</span>',
    unsafe_allow_html=True,
)
st.caption("Dati live: Binance → Kafka → Spark → PostgreSQL → questa dashboard")

# Sidebar: controlli
with st.sidebar:
    st.header("⚙️ Controlli")
    minutes = st.slider("Finestra storico (minuti)", 1, 60, 10)
    refresh_s = st.slider("Auto-refresh (secondi)", 2, 30, 5)
    st.markdown("---")
    st.write("Fonte: tabella `crypto_prices` / `crypto_agg_1min`")

# Forza il rerun ogni refresh_s secondi -> dati sempre aggiornati
st_autorefresh(interval=refresh_s * 1000, key="auto")

# --- 1) Metriche: prezzo corrente per simbolo --------------------------------
latest = query_df(
    """
    SELECT DISTINCT ON (symbol) symbol, price, event_time
    FROM crypto_prices
    ORDER BY symbol, event_time DESC
    """
)

if latest.empty:
    st.warning("Nessun dato in `crypto_prices`. È attivo il producer e gira Spark?")
    st.stop()

# Prezzo di riferimento all'inizio della finestra, per calcolare il delta %
ref = query_df(
    """
    SELECT DISTINCT ON (symbol) symbol, price AS ref_price
    FROM crypto_prices
    WHERE event_time > now() - make_interval(mins => :m)
    ORDER BY symbol, event_time ASC
    """,
    {"m": minutes},
)
merged = latest.merge(ref, on="symbol", how="left")

cols = st.columns(len(merged))
for col, row in zip(cols, merged.itertuples()):
    delta = None
    if row.ref_price:
        pct = (row.price - row.ref_price) / row.ref_price * 100
        delta = f"{pct:+.2f}% ({minutes}m)"
    label = COIN_LABEL.get(row.symbol, row.symbol)
    # st.metric colora da solo il delta: verde se positivo, rosso se negativo.
    col.metric(label=label, value=f"{row.price:,.2f}", delta=delta)

st.caption(f"Ultimo aggiornamento dato: {latest['event_time'].max()}")
st.markdown("---")

# --- 2) Selettore simbolo ----------------------------------------------------
symbols = sorted(latest["symbol"].tolist())
sel = st.selectbox(
    "Simbolo da visualizzare",
    symbols,
    format_func=lambda s: COIN_LABEL.get(s, s),
)

# --- 3) Grafico AREA del prezzo (tick grezzi) --------------------------------
hist = query_df(
    """
    SELECT event_time, price
    FROM crypto_prices
    WHERE symbol = :s AND event_time > now() - make_interval(mins => :m)
    ORDER BY event_time
    """,
    {"s": sel, "m": minutes},
)

left, right = st.columns([2, 1])
with left:
    st.subheader(f"Andamento prezzo — {COIN_LABEL.get(sel, sel)}")
    if hist.empty:
        st.info("Nessun dato nella finestra selezionata.")
    else:
        # Colore secondo il trend nella finestra: verde se sale, rosso se scende.
        rising = hist["price"].iloc[-1] >= hist["price"].iloc[0]
        line_color = GREEN if rising else RED
        fill_color = (
            "rgba(38,166,154,0.20)" if rising else "rgba(239,83,80,0.20)"
        )
        fig = go.Figure(
            go.Scatter(
                x=hist["event_time"],
                y=hist["price"],
                mode="lines",
                line=dict(color=line_color, width=2),
                fill="tozeroy",
                fillcolor=fill_color,
                hovertemplate="%{y:,.2f}<extra></extra>",
            )
        )
        # Zoom verticale "stretto" sui dati: l'area sotto la linea resta un
        # gradiente elegante invece di schiacciarsi verso lo zero.
        lo, hi = hist["price"].min(), hist["price"].max()
        pad = (hi - lo) * 0.15 or hi * 0.001
        fig.update_yaxes(range=[lo - pad, hi + pad])
        st.plotly_chart(style_fig(fig), use_container_width=True, config={"displayModeBar": False})

# --- 4) Candlestick + tabella dagli aggregati 1-min --------------------------
agg = query_df(
    """
    SELECT window_start, avg_price, min_price, max_price, n_ticks
    FROM crypto_agg_1min
    WHERE symbol = :s
    ORDER BY window_start DESC
    LIMIT 30
    """,
    {"s": sel},
)

with right:
    st.subheader("Candele 1 min")
    if agg.empty:
        st.info("Aggregati non ancora disponibili (servono ~1-2 min di dati).")
    else:
        candle = agg.sort_values("window_start").copy()
        # Non abbiamo OHLC reali: ricostruiamo candele oneste dagli aggregati.
        #   high = max, low = min, close = media della finestra,
        #   open = media della finestra PRECEDENTE (prima candela = doji).
        candle["close"] = candle["avg_price"]
        candle["open"] = candle["close"].shift(1).fillna(candle["close"])
        fig_c = go.Figure(
            go.Candlestick(
                x=candle["window_start"],
                open=candle["open"],
                high=candle["max_price"],
                low=candle["min_price"],
                close=candle["close"],
                increasing_line_color=GREEN,
                decreasing_line_color=RED,
            )
        )
        fig_c.update_layout(xaxis_rangeslider_visible=False)
        st.plotly_chart(style_fig(fig_c), use_container_width=True, config={"displayModeBar": False})

# Tabella aggregati sotto, a tutta larghezza, per il dettaglio numerico.
if not agg.empty:
    with st.expander("📋 Dettaglio aggregati 1 min (ultimi 30)"):
        st.dataframe(
            agg.round(2),
            hide_index=True,
            use_container_width=True,
        )
