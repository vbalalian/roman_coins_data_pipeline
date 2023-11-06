import pandas as pd
import numpy as np
from fastapi import FastAPI
from typing import Optional

app = FastAPI()

df = pd.read_csv('/home/vbalalian/RomanCoins/web_scraping/roman_coins.csv')
df.replace([np.inf, -np.inf, np.nan], None, inplace=True) # Handle missing values

@app.get('/')
async def root():
    return {
        'message':'Welcome to the Roman Coin Data API',
        'status':'OK',
        'documentation':'/docs',
        "endpoints": {
            "/coins": "Retrieve a paginated list of coins with optional sorting and filtering",
            "/coins/id/{coin_id}": "Retrieve detailed information about a single coin by its ID",
            "/coins/search": "Search for coins based on query"
            }
        }

@app.get("/coins/")
async def read_coins(
    page: int = 1, 
    page_size: int = 10, 
    sort_by: Optional[str] = None,
    metal: Optional[str] = None,
    era: Optional[str] = None,
    year: Optional[str] = None

    ):

    start = (page - 1) * page_size
    end = start + page_size
    data = df

    if metal:
        data = data[data['metal'] == metal]
    if era:
        data = data[data['era'] == era]
    if year:
        data = data[data['year'].astype(str) == year]
    if sort_by:
        data = data.sort_values(by=sort_by)
    return data.iloc[start:end].to_dict(orient='records')

@app.get("/coins/search")
async def search_coins(query: Optional[str] = None):
    if query:
        search_result = df[df['description'].str.contains(query, na=False)]
        return search_result.to_dict(orient='records')
    return {"error": "Query string is empty"}

@app.get("/coins/id/{coin_id}")
async def read_coin(coin_id: str):
    coin = df[df['id'] == coin_id]
    if coin.empty:
        return {"error": "Coin not found"}
    return coin.iloc[0].to_dict()