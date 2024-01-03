![roman_counting_coins](https://github.com/vbalalian/RomanCoins/assets/120220346/d52d3ba8-1f29-488a-82ec-9de71460daaa)

# Roman Coins
## A (work in progress) end-to-end data pipeline project
[![Continuous Integration](https://github.com/vbalalian/RomanCoins/actions/workflows/ci.yml/badge.svg)](https://github.com/vbalalian/RomanCoins/actions/workflows/ci.yml)

Extracting, Loading, and Transforming data on Roman Coins gathered from wildwinds.com

**Tools:** Python, PostgreSQL, Docker, FastAPI, BeautifulSoup

### [Web Scraper](web_scraping/web_scraper.py)

Scrapes data on coins from the Roman Empire from wildwinds.com, and loads the data into a postgres server. Due to the required 30-second delay between page requests, scraping takes several hours to complete; the data is loaded into the server as it arrives.

### [API](api/main.py)

Serves data from the roman coins dataset, and allows data addition and manipulation via POST, PUT, and PATCH endpoints. Data is continuously added during web scraping. 

### [Airbyte](custom_airbyte_connector/source_roman_coin_api/source.py)

Custom Airbyte connector streams incremental data from the api.

## Requirements:

[Docker](https://docs.docker.com/engine/install/)\
[Docker Compose](https://docs.docker.com/compose/install/)\
[Airbyte](https://docs.airbyte.com/deploying-airbyte/local-deployment)

## To Run:

Terminal Commands:
```
git clone https://github.com/vbalalian/roman_coins_data_pipeline.git
cd roman_coins_data_pipeline
docker compose up -d
```
Access version 1 of the API at http://localhost:8010/v1/ \
(Try out the different endpoints using the interactive documentation at http://localhost:8010/v1/docs)
