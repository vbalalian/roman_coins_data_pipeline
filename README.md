# A (work in progress) end-to-end data pipeline project
Extracting, Loading, and Transforming data on Roman Coins gathered from wildwinds.com

### [Web](web_scraping/web_scraper.ipynb) [Scraper](web_scraping/web_scraper.py)
**Tools:** BeautifulSoup, Pandas, Jupyter Notebook  
**Skills:** Web Scraping, Data Cleaning, Data Visualization

wildwinds.com, written in html, has extensive data on ancient coins organized into various periods. Much of the data is mixed among string descriptions. This webscraper scrapes data from the website section dedicated to coins from of the Roman Empire, which are contained on separate pages for each emperor/figurehead.

### [API](roman_coin_api/main.py)
**Tools:** FastAPI  