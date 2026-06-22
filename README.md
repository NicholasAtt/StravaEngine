# StravaEngine

## Descrizione

StravaEngine è una pipeline locale per acquisire, simulare, processare e monitorare in streaming una corsa Strava arricchita con dati ambientali.

Il progetto usa un'architettura a microservizi orchestrata con Docker Compose. I dati della corsa vengono recuperati da Strava, inviati a Logstash, pubblicati su Kafka, elaborati da Spark Structured Streaming e infine salvati su Redis ed Elasticsearch per dashboard live, storico ed alerting.

L'obiettivo è costruire una pipeline end-to-end osservabile per un singolo runner:

- recuperare attivita reali da Strava;
- simulare lo streaming dei punti GPS e biometrici;
- arricchire ogni punto con meteo e qualita dell'aria;
- stimare la frequenza cardiaca attesa con un modello Spark ML;
- individuare possibili condizioni di fatica;
- visualizzare metriche live e storiche in Grafana.

## Setup

Per eseguire il progetto in locale è necessario avere installato **Docker** con Docker Compose.

```bash
# Clonare il repository
git clone <repository-url>

# Spostarsi nella directory del progetto
cd StravaEngine

# Preparare il file ambiente
cp .env.example .env
```

Compilare il file `.env` con almeno:

```text
GRAFANA_ADMIN_PASSWORD=change_me
STRAVA_TOKEN=your_strava_access_token_here
GF_SMTP_ENABLED=false
```

Il file `.env` contiene segreti locali e non deve essere committato.

### Recupero del token Strava

La pipeline usa `STRAVA_TOKEN` per chiamare le API Strava. Strava usa OAuth2: l'access token è a breve durata, mentre il refresh token serve per generarne uno nuovo quando scade. Il progetto al momento legge solo `STRAVA_TOKEN`, quindi se il token scade bisogna aggiornarlo manualmente nel file `.env`.

Riferimento ufficiale Strava: https://developers.strava.com/docs/authentication

1. Creare un'app Strava da:

```text
https://www.strava.com/settings/api
```

2. Impostare come callback domain:

```text
localhost
```

3. Aprire nel browser questo URL sostituendo `YOUR_CLIENT_ID`:

```text
https://www.strava.com/oauth/authorize?client_id=YOUR_CLIENT_ID&response_type=code&redirect_uri=http://localhost/exchange_token&approval_prompt=force&scope=read,read_all,profile:read_all,profile:write,activity:read,activity:read_all,activity:write
```

4. Autorizzare l'app. Il browser verra rediretto a un URL simile a:

```text
http://localhost/exchange_token?state=&code=AUTHORIZATION_CODE&scope=read,activity:write,activity:read,activity:read_all,profile:write,profile:read_all,read_all
```

5. Copiare il valore del parametro `AUTHORIZATION_CODE` e scambiarlo con un access token:

```bash
curl -X POST https://www.strava.com/oauth/token \
  -F client_id=YOUR_CLIENT_ID \
  -F client_secret=YOUR_CLIENT_SECRET \
  -F code=AUTHORIZATION_CODE \
  -F grant_type=authorization_code
```

6. Copiare `access_token` nella variabile `STRAVA_TOKEN` del file `.env`.

Per rigenerare un access token usando il refresh token:

```bash
curl -X POST https://www.strava.com/oauth/token \
  -F client_id=YOUR_CLIENT_ID \
  -F client_secret=YOUR_CLIENT_SECRET \
  -F refresh_token=YOUR_REFRESH_TOKEN \
  -F grant_type=refresh_token
```

Dopo aver modificato `.env`, ricreare almeno il container che usa il token:

```bash
docker compose up -d --force-recreate strava-injector
```

## Avvio del progetto

Avviare tutti i container:

```bash
docker compose up -d --build
```

Verificare lo stato dei servizi:

```bash
docker compose ps
```

Servizi principali esposti sull'host:

- Grafana: http://localhost:3000
- Elasticsearch: http://localhost:9200
- Logstash HTTP input: http://localhost:8080
- Spark UI: http://localhost:4040

Credenziali Grafana:

- utente: `admin`
- password: valore di `GRAFANA_ADMIN_PASSWORD` in `.env`

Per fermare l'ambiente:

```bash
docker compose down
```

Per fermarlo eliminando anche i volumi persistenti:

```bash
docker compose down -v
```

## Replay di una corsa Strava

Il container `strava-injector` è un utility container che rimane in attesa. Per lanciare una simulazione:

```bash
docker compose exec strava-injector python scripts/strava_replay.py
```

Lo script:

- legge le attività disponibili tramite Strava API;
- mostra una lista numerata;
- chiede quale attività iniettare;
- scarica stream GPS, altitudine, frequenza cardiaca, cadenza e distanza;
- scarica dati meteo e qualità dell'aria da Open-Meteo per data e posizione della corsa;
- invia gli eventi a Logstash rispettando il ritmo temporale originale della corsa;
- pulisce gli stream live Redis al termine.

Per simulare la corsa usando timestamp correnti:

```bash
docker compose exec strava-injector python scripts/strava_replay.py --realtime
```


## Tecnologie e infrastruttura

Il progetto è composto dai seguenti servizi:

- **Logstash**: espone un input HTTP sulla porta `8080`, normalizza gli eventi e li instrada sui topic Kafka corretti.
- **Kafka**: disaccoppia ingestione e processing tramite tre topic: `running-live-data`, `weatherdata`, `airquality`.
- **Spark**: legge i topic Kafka con Structured Streaming, arricchisce i punti corsa, applica il modello ML, scrive su Redis ed Elasticsearch.
- **Redis**: mantiene lo stato live a bassa latenza, la cache ambientale corrente e la soglia dinamica del modello.
- **Elasticsearch**: conserva dati arricchiti, riepiloghi sessione e documenti interrogabili da Grafana.
- **Grafana**: visualizza dashboard live e storiche. gestisce alerting.
- **Open-Meteo**: fornisce dati meteo e qualita dell'aria.
- **Strava API**: fornisce attività, GPS stream e metriche biometriche.

## Flusso dati

```text
Strava API + Open-Meteo
        |
        v
strava-injector
        |
        v
Logstash HTTP input
        |
        v
Kafka
|-- running-live-data
|-- weatherdata
`-- airquality
        |
        v
Spark Structured Streaming
        |
        |-- Redis, per dashboard live e cache operativa
        |-- Elasticsearch, per storico e alert Grafana
```

## Topic Kafka

I topic vengono creati automaticamente dal container `kafka-setup`:

- `running-live-data`: punti corsa Strava ad alta frequenza;
- `weatherdata`: dati meteo orari;
- `airquality`: dati qualità aria orari.

Nota: i messaggi Kafka si interrogano dal container `broker`; il container `spark` li consuma e mostra nei log solo il processing.

Elencare i topic:

```bash
docker compose exec broker /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --list
```

Leggere i messaggi del topic Strava dall'inizio:

```bash
docker compose exec broker /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic running-live-data \
  --from-beginning
```

Leggere solo pochi messaggi e terminare:

```bash
docker compose exec broker /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic running-live-data \
  --from-beginning \
  --max-messages 5
```

Leggere i topic ambientali:

```bash
docker compose exec broker /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic weatherdata \
  --from-beginning \
  --max-messages 5

docker compose exec broker /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic airquality \
  --from-beginning \
  --max-messages 5
```

Descrivere un topic:

```bash
docker compose exec broker /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --topic running-live-data
```

## Logstash

La configurazione Logstash si trova in:

```text
logstash/pipeline/logstash.conf
```

Logstash riceve eventi JSON da `strava-injector`, applica conversioni e rename dei campi e invia gli eventi a Kafka:

- `stream_type=strava` va su `running-live-data`;
- `stream_type=weather` va su `weatherdata`;
- `stream_type=airquality` va su `airquality`.

Vedere i log di Logstash:

```bash
docker compose logs -f logstash
```

Poichè la pipeline include `stdout { codec => rubydebug }`, nei log di Logstash si vedono anche gli eventi ricevuti e normalizzati.

Inviare un evento di test a Logstash:

```bash
curl -X POST http://localhost:8080 \
  -H "Content-Type: application/json" \
  -d '{"stream_type":"weather","session_id":"test","time":"2026-01-01T10:00:00Z","temperature":20.0,"humidity":55.0}'
```

Dopo il test, verificare il topic:

```bash
docker compose exec broker /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic weatherdata \
  --from-beginning \
  --max-messages 1
```

## Spark

Il container `spark` avvia automaticamente il job streaming definito in:

```text
spark/src/app.py
```

Vedere i log Spark:

```bash
docker compose logs -f spark
```

Aprire la Spark UI:

```text
http://localhost:4040
```

Il job Spark legge i topic Kafka, mantiene una cache ambientale in Redis e scrive:

- stream live su Redis;
- punti arricchiti su Elasticsearch;
- riepiloghi sessione su Elasticsearch;
- aggregazioni temporali su Redis.

Chiavi Redis principali:

- `env:current`: temperatura, umidita e AQI correnti;
- `race:live_tracking`: stream live con posizione;
- `race:live_metrics`: stream live con HR reale, HR atteso e fatica;
- `race:cardiac_drift_stream`: aggregazioni di efficienza;
- `model:fatigue_threshold`: soglia di fatica salvata dal training.

Ispezionare Redis:

```bash
docker compose exec redis redis-cli XREVRANGE race:live_tracking + - COUNT 5
docker compose exec redis redis-cli XREVRANGE race:live_metrics + - COUNT 5
docker compose exec redis redis-cli HGETALL env:current
docker compose exec redis redis-cli GET model:fatigue_threshold
```

Interrogare Elasticsearch:

```bash
curl "http://localhost:9200/strava_metrics/_search?pretty&size=5"
```

## Training del modello

Il modello viene addestrato con:

```text
spark/src/train_model.py
```

Dataset di input:

```text
spark/dataset/historical_runs.csv
```

`historical_runs.csv` è un dataset sintetico. Simula le correlazioni tra parametri fisici e ambientali per fornire al modello la logica di base necessaria a calcolare l'affaticamento in tempo reale.

Feature usate:

- `cadence`
- `elevation`
- `distance`
- `temperature`
- `humidity`
- `aqi`

Label:

- `heart_rate`

Il modello è una pipeline Spark ML composta da `VectorAssembler` e `RandomForestRegressor`. L'output viene salvato nel volume Docker `spark_data`:

```text
/opt/spark-data/models/hr_fatigue_model
```

Eseguire il training:

```bash
docker compose exec spark /usr/local/spark/bin/spark-submit /opt/spark-src/train_model.py
```

Durante il training vengono generati:

- modello in `/opt/spark-data/models/hr_fatigue_model`;
- validation set in `/opt/spark-data/dataset_validation`;
- soglia `model:fatigue_threshold` in Redis, calcolata a partire dall'RMSE.

Validare il modello:

```bash
docker compose exec spark /usr/local/spark/bin/spark-submit /opt/spark-src/validate_model.py
```

La validazione stampa:

- importanza delle feature;
- RMSE;
- MAE;
- R2;
- prime predizioni sul validation set.

Il job streaming carica il modello solo all'avvio. Dopo un nuovo training, riavviare Spark per usare il modello aggiornato:

```bash
docker compose restart spark
```

## Dashboard e alerting

Grafana viene configurato automaticamente dai file in:

```text
docker/grafana/provisioning
```

File principali:

- `datasources/datasources.yaml`: datasource Redis ed Elasticsearch;
- `dashboards/json/dashboard.json`: dashboard live;
- `dashboards/json/dashboard_history.json`: dashboard storica;
- `alerting/alerting.yaml`: regole di alert.

La dashboard live legge soprattutto Redis, mentre la dashboard storica legge Elasticsearch.


## Struttura del repository

```text
StravaEngine/
|-- docker-compose.yml
|
|-- docker/
|   |-- grafana/
|   |   `-- provisioning/
|   |-- scripts/
|   |   |-- Dockerfile
|   |   `-- requirements.txt
|   `-- spark/
|       |-- Dockerfile
|       `-- requirements.txt
|
|-- logstash/
|   `-- pipeline/
|       `-- logstash.conf
|
|-- scripts/
|   |-- strava_replay.py
|   |-- strava_ingestor.py
|   `-- env_ingestor.py
|
|-- spark/
|   |-- dataset/
|   |   `-- historical_runs.csv
|   `-- src/
|       |-- app.py
|       |-- train_model.py
|       `-- validate_model.py
|
|-- .env.example
`-- README.md
```
