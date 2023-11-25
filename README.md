# A (work in progress) end-to-end data pipeline project
Extracting, Loading, and Transforming data on Roman Coins gathered from wildwinds.com

**Tools:** Python, PostgreSQL, Docker, FastAPI, BeautifulSoup, Pandas

### [Web](web_scraping/web_scraper.ipynb) [Scraper](web_scraping/web_scraper.py)

Scrapes data on coins from the Roman Empire from wildwinds.com, and loads the data into a postgres server.

### [API](roman_coin_api/main.py)

Serves data from the roman coins dataset, and allows data ingestion via POST endpoint.

## To Run:
**(Requires Docker with Docker Compose)**

Terminal Commands:
1) git clone https://github.com/vbalalian/RomanCoins
2) cd RomanCoins
3) docker compose up
