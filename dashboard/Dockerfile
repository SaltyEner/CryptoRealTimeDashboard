# Dockerfile della dashboard Streamlit (Passo 6)
FROM python:3.12-slim

WORKDIR /app

# Installiamo prima le dipendenze (layer cache: cambia di rado)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Poi il tema e il codice dell'app
COPY .streamlit ./.streamlit
COPY app.py .

EXPOSE 8501

# Avvio di Streamlit in ascolto su tutte le interfacce del container
CMD ["streamlit", "run", "app.py", \
     "--server.port=8501", "--server.address=0.0.0.0", \
     "--browser.gatherUsageStats=false"]
