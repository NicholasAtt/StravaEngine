# StravaEngine

## Description

StravaEngine is a local pipeline to acquire, simulate, process, and monitor a Strava run enriched with environmental data in streaming.

The project uses a microservices architecture orchestrated with Docker Compose. Run data is retrieved from Strava, sent to Logstash, published on Kafka, processed by Spark Structured Streaming, and finally saved to Redis and Elasticsearch for live dashboards, historical data, and alerting.

The goal is to build an observable end-to-end pipeline for a single runner:

- retrieve real activities from Strava;
- simulate the streaming of GPS and biometric points;
- enrich each point with weather and air quality data;
- estimate the expected heart rate using a Spark ML model;
- identify possible fatigue conditions;
- visualize live and historical metrics in Grafana.

## Setup

To run the project locally, you need to have **Docker** with Docker Compose installed.

```bash
# Clone the repository
git clone <repository-url>

# Navigate into the project directory
cd StravaEngine

# Prepare the environment file
cp .env.example .env
```

Fill in the `.env` file with at least:

```text
GRAFANA_ADMIN_PASSWORD=change_me
STRAVA_TOKEN=your_strava_access_token_here
GF_SMTP_ENABLED=false
```

The `.env` file contains local secrets and must not be committed.

### Retrieving the Strava token

The pipeline uses `STRAVA_TOKEN` to call Strava APIs. Strava uses OAuth2: the access token is short-lived, while the refresh token is used to generate a new one when it expires. The project currently only reads `STRAVA_TOKEN`, so if the token expires, it must be updated manually in the `.env` file.

Official Strava reference: https://developers.strava.com/docs/authentication

1. Create a Strava app from:

```text
https://www.strava.com/settings/api
```

2. Set as callback domain:

```text
localhost
```

3. Open this URL in the browser, replacing `YOUR_CLIENT_ID`:

```text
https://www.strava.com/oauth/authorize?client_id=YOUR_CLIENT_ID&response_type=code&redirect_uri=http://localhost/exchange_token&approval_prompt=force&scope=read,read_all,profile:read_all,profile:write,activity:read,activity:read_all,activity:write
```

4. Authorize the app. The browser will be redirected to a URL similar to:

```text
http://localhost/exchange_token?state=&code=AUTHORIZATION_CODE&scope=read,activity:write,activity:read,activity:read_all,profile:write,profile:read_all,read_all
```

5. Copy the value of the `AUTHORIZATION_CODE` parameter and exchange it for an access token:

```bash
curl -X POST https://www.strava.com/oauth/token \
  -F client_id=YOUR_CLIENT_ID \
  -F client_secret=YOUR_CLIENT_SECRET \
  -F code=AUTHORIZATION_CODE \
  -F grant_type=authorization_code
```

6. Copy the `access_token` into the `STRAVA_TOKEN` variable in the `.env` file.

To regenerate an access token using the refresh token:

```bash
curl -X POST https://www.strava.com/oauth/token \
  -F client_id=YOUR_CLIENT_ID \
  -F client_secret=YOUR_CLIENT_SECRET \
  -F refresh_token=YOUR_REFRESH_TOKEN \
  -F grant_type=refresh_token
```

After modifying `.env`, recreate at least the container that uses the token:

```bash
docker compose up -d --force-recreate strava-injector
```

## Starting the project

Start all containers:

```bash
docker compose up -d --build
```

Check the status of the services:

```bash
docker compose ps
```

Main services exposed on the host:

- Grafana: http://localhost:3000
- Elasticsearch: http://localhost:9200
- Logstash HTTP input: http://localhost:8080
- Spark UI: http://localhost:4040

Grafana credentials:

- user: `admin`
- password: value of `GRAFANA_ADMIN_PASSWORD` in `.env`

To stop the environment:

```bash
docker compose down
```

To stop it and also delete persistent volumes:

```bash
docker compose down -v
```

## Strava run replay

The `strava-injector` container is a utility container that waits in the background. To launch a simulation:

```bash
docker compose exec strava-injector python scripts/strava_replay.py
```

The script:

- reads available activities via Strava API;
- shows a numbered list;
- asks which activity to inject;
- downloads GPS, altitude, heart rate, cadence, and distance streams;
- downloads weather and air quality data from Open-Meteo for the date and location of the run;
- sends events to Logstash respecting the original time pace of the run;
- cleans up Redis live streams when finished.

To simulate the run using current timestamps:

```bash
docker compose exec strava-injector python scripts/strava_replay.py --realtime
```

## Technologies and infrastructure

The project consists of the following services:

- **Logstash**: exposes an HTTP input on port `8080`, normalizes events, and routes them to the correct Kafka topics.
- **Kafka**: decouples ingestion and processing via three topics: `running-live-data`, `weatherdata`, `airquality`.
- **Spark**: reads Kafka topics with Structured Streaming, enriches run points, applies the ML model, writes to Redis and Elasticsearch.
- **Redis**: maintains the low-latency live state, the current environmental cache, and the dynamic model threshold.
- **Elasticsearch**: stores enriched data, session summaries, and queryable documents for Grafana.
- **Grafana**: displays live and historical dashboards, manages alerting.
- **Open-Meteo**: provides weather and air quality data.
- **Strava API**: provides activities, GPS streams, and biometric metrics.

## Data flow

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
        |-- Redis, for live dashboards and operational cache
        |-- Elasticsearch, for history and Grafana alerts
```

## Kafka topics

Topics are created automatically by the `kafka-setup` container:

- `running-live-data`: high-frequency Strava run points;
- `weatherdata`: hourly weather data;
- `airquality`: hourly air quality data.

Note: Kafka messages are queried from the `broker` container; the `spark` container consumes them and only shows the processing in the logs.

List topics:

```bash
docker compose exec broker /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --list
```

Read messages from the Strava topic from the beginning:

```bash
docker compose exec broker /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic running-live-data \
  --from-beginning
```

Read only a few messages and terminate:

```bash
docker compose exec broker /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic running-live-data \
  --from-beginning \
  --max-messages 5
```

Read environmental topics:

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

Describe a topic:

```bash
docker compose exec broker /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --topic running-live-data
```

## Logstash

The Logstash configuration is located in:

```text
logstash/pipeline/logstash.conf
```

Logstash receives JSON events from `strava-injector`, applies conversions and field renaming, and sends the events to Kafka:

- `stream_type=strava` goes to `running-live-data`;
- `stream_type=weather` goes to `weatherdata`;
- `stream_type=airquality` goes to `airquality`.

View Logstash logs:

```bash
docker compose logs -f logstash
```

Since the pipeline includes `stdout { codec => rubydebug }`, the received and normalized events are also visible in the Logstash logs.

Send a test event to Logstash:

```bash
curl -X POST http://localhost:8080 \
  -H "Content-Type: application/json" \
  -d '{"stream_type":"weather","session_id":"test","time":"2026-01-01T10:00:00Z","temperature":20.0,"humidity":55.0}'
```

After testing, verify the topic:

```bash
docker compose exec broker /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic weatherdata \
  --from-beginning \
  --max-messages 1
```

## Spark

The `spark` container automatically starts the streaming job defined in:

```text
spark/src/app.py
```

View Spark logs:

```bash
docker compose logs -f spark
```

Open the Spark UI:

```text
http://localhost:4040
```

The Spark job reads the Kafka topics, maintains an environmental cache in Redis, and writes:

- live streams to Redis;
- enriched points to Elasticsearch;
- session summaries to Elasticsearch;
- temporal aggregations to Redis.

Main Redis keys:

- `env:current`: current temperature, humidity, and AQI;
- `race:live_tracking`: live stream with position;
- `race:live_metrics`: live stream with real HR, expected HR, and fatigue;
- `race:cardiac_drift_stream`: efficiency aggregations;
- `model:fatigue_threshold`: fatigue threshold saved from training.

Inspect Redis:

```bash
docker compose exec redis redis-cli XREVRANGE race:live_tracking + - COUNT 5
docker compose exec redis redis-cli XREVRANGE race:live_metrics + - COUNT 5
docker compose exec redis redis-cli HGETALL env:current
docker compose exec redis redis-cli GET model:fatigue_threshold
```

Query Elasticsearch:

```bash
curl "http://localhost:9200/strava_metrics/_search?pretty&size=5"
```

## Model training

The model is trained with:

```text
spark/src/train_model.py
```

Input dataset:

```text
spark/dataset/historical_runs.csv
```

`historical_runs.csv` is a synthetic dataset. It simulates correlations between physical and environmental parameters to provide the model with the basic logic needed to calculate fatigue in real-time.

Features used:

- `cadence`
- `elevation`
- `distance`
- `temperature`
- `humidity`
- `aqi`

Label:

- `heart_rate`

The model is a Spark ML pipeline composed of `VectorAssembler` and `RandomForestRegressor`. The output is saved in the `spark_data` Docker volume:

```text
/opt/spark-data/models/hr_fatigue_model
```

Run training:

```bash
docker compose exec spark /usr/local/spark/bin/spark-submit /opt/spark-src/train_model.py
```

During training, the following are generated:

- model in `/opt/spark-data/models/hr_fatigue_model`;
- validation set in `/opt/spark-data/dataset_validation`;
- `model:fatigue_threshold` in Redis, calculated from RMSE.

Validate the model:

```bash
docker compose exec spark /usr/local/spark/bin/spark-submit /opt/spark-src/validate_model.py
```

The validation prints:

- feature importance;
- RMSE;
- MAE;
- R2;
- first predictions on the validation set.

The streaming job loads the model only at startup. After a new training, restart Spark to use the updated model:

```bash
docker compose restart spark
```

## Dashboards and alerting

Grafana is automatically configured by the files in:

```text
docker/grafana/provisioning
```

Main files:

- `datasources/datasources.yaml`: Redis and Elasticsearch datasources;
- `dashboards/json/dashboard.json`: live dashboard;
- `dashboards/json/dashboard_history.json`: historical dashboard;
- `alerting/alerting.yaml`: alert rules.

The live dashboard mainly reads from Redis, while the historical dashboard reads from Elasticsearch.


## Repository structure

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
