# 🚢 Passo 8 (light) — Producer + Kafka su Kubernetes (minikube)

Questa cartella dimostra come portare una **fetta** della pipeline da Docker
Compose a **Kubernetes**, eseguito in locale con **minikube**. Scegliamo una
fetta **auto-contenuta**: il *producer* che pubblica su *Kafka*, entrambi dentro
il cluster. Niente dipendenze dal resto dello stack.

## Cosa impari qui (i mattoni di Kubernetes)

| File | Risorse k8s | Concetto |
|---|---|---|
| `00-namespace.yaml` | Namespace | isolare le risorse del progetto |
| `10-kafka.yaml` | Deployment + Service | un pod gestito + un nome DNS stabile (`kafka`) |
| `20-producer.yaml` | Deployment + ConfigMap | un pod + la configurazione separata dal codice |

Concetto chiave: il producer trova Kafka **per nome** (`kafka:9092`) grazie al
*Service*, esattamente come in Compose i servizi si vedevano per nome. Kubernetes
in più: riavvia i pod che cadono, sa scalare le repliche, gira su più nodi.

---

## Prerequisiti

- **Docker Desktop** attivo (minikube lo usa come "motore").
- **kubectl** (già presente).
- **minikube** da installare (vedi sotto).

### Installare minikube (Windows, PowerShell)

```powershell
winget install Kubernetes.minikube
```

Poi **chiudi e riapri il terminale** (per ricaricare il PATH) e verifica:

```bash
minikube version
```

---

## Avvio passo-passo

```bash
# 1) Avvia il mini-cluster (usa Docker come driver)
minikube start --driver=docker

# 2) Carica l'immagine LOCALE del producer dentro il cluster.
#    (minikube ha un suo registro: le immagini del Docker host non le vede)
minikube image load progetto3-producer:latest

# 3) Applica i manifest (Kubernetes legge tutti gli YAML della cartella)
kubectl apply -f k8s/

# 4) Guarda i pod nascere e diventare "Running" (Ctrl+C per uscire dal watch)
kubectl get pods -n crypto -w
```

Al primo avvio Kafka ci mette ~30-60s a diventare `Running`/`Ready` (scarica
l'immagine e inizializza KRaft). Il producer potrebbe riavviarsi un paio di
volte finché Kafka non è pronto: è normale (Kubernetes lo **ri-tenta da solo**).

---

## Verifica che funzioni

**A) I log del producer** — devi vedere i prezzi scorrere:

```bash
kubectl logs -n crypto deploy/producer -f
```

**B) I messaggi davvero dentro Kafka** — leggiamo dal topic con il
console-consumer ufficiale, eseguito DENTRO il pod di Kafka:

```bash
kubectl exec -n crypto deploy/kafka -- \
  /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic crypto-prices \
  --from-beginning --max-messages 5
```

Se vedi 5 messaggi JSON con `symbol`/`price`, la fetta gira su Kubernetes. ✅

---

## Comandi utili (cassetta degli attrezzi kubectl)

```bash
kubectl get all -n crypto              # panoramica di tutte le risorse
kubectl describe pod -n crypto <nome>  # dettagli/eventi di un pod (debug)
kubectl logs -n crypto <pod>           # log di un pod
kubectl scale -n crypto deploy/producer --replicas=2   # prova a scalare!
```

## Pulizia

```bash
kubectl delete -f k8s/     # rimuove solo le risorse del progetto
minikube stop              # ferma il cluster (lo riusi dopo con 'start')
minikube delete            # cancella del tutto il cluster
```

> 💡 Questa è la versione *light*: dimostra il workflow Kubernetes su una fetta
> reale e viva. Lo stack completo resta avviabile con Docker Compose
> (`docker compose up -d`), che per sviluppo e demo è più immediato.
