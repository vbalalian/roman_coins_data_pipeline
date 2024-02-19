![roman_counting_coins](https://github.com/vbalalian/RomanCoins/assets/120220346/d52d3ba8-1f29-488a-82ec-9de71460daaa)

# Roman Coins
## End-to-end ELT pipeline project
[![Continuous Integration](https://github.com/vbalalian/RomanCoins/actions/workflows/ci.yml/badge.svg)](https://github.com/vbalalian/RomanCoins/actions/workflows/ci.yml)

Extracting, Loading, and Transforming data on Roman Coins gathered from wildwinds.com

**Tools:** Python, PostgreSQL, Docker, FastAPI, Airbyte, MinIO, Dagster

### [Web Scraper](web_scraping/web_scraper.py)

Scrapes data on coins from the Roman Empire from wildwinds.com, and loads the data into a postgres server. Due to the required 30-second delay between page requests, scraping takes several hours to complete; the data is loaded into the server as it arrives.

### [API](api/main.py)

Serves data from the roman coins dataset, and allows data addition and manipulation via POST, PUT, and PATCH endpoints. Data is continuously added during web scraping. 

### [Airbyte](airbyte-api-minio-connection/airbyte_connection_config.py)

[Custom airbyte connector](custom-airbyte-connector/source_roman_coin_api/source.py) streams incremental data from the API to a standalone MinIO bucket.

### [MinIO](https://min.io)

Resilient storage for the incoming data stream. Data is replicated ["at least once"](https://docs.airbyte.com/using-airbyte/core-concepts/sync-modes/incremental-append-deduped#inclusive-cursors) by Airbyte, so some duplicated data is acceptable at this stage. Deduplication will be easily handled by dbt at the next stage of the pipeline.

### [Dagster](orchestration)

Automatically triggers Airbyte syncs every 30 minutes. 

## Requirements:

[Docker](https://docs.docker.com/engine/install/)\
[Docker Compose](https://docs.docker.com/compose/install/)\
[Airbyte](https://docs.airbyte.com/deploying-airbyte/local-deployment)

## To Run:

**Step 1:** Ensure Docker and Airbyte are both up and running.

**Step 2: (Optional)** Set preferred credentials/variables in project .env file

**Step 3:** Run the following terminal commands:
```
git clone https://github.com/vbalalian/roman_coins_data_pipeline.git
cd roman_coins_data_pipeline
docker compose up
```
This will run the web scraper, the API, MinIO, and [Dagster](https://dagster.io); then build the custom Airbyte connector, configure the API-Airbyte-Minio connection, and trigger Airbyte syncs every 30 minutes.

- View the web_scraper container logs in Docker to follow the progress of the Web Scraping

- Access the API directly at http://localhost:8010, or interact with the different endpoints at http://localhost:8010/docs

- Access the Airbyte UI at http://localhost:8000

- Access the MinIO Console at http://localhost:9090

- Access the Dagster UI at http://localhost:3000
