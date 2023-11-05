import pandas as pd
from fastapi import FastAPI

app = FastAPI()

df = pd.read_csv('/home/vbalalian/RomanCoins/web_scraping/roman_coins.csv')

@app.get('/')
def read_root():
    return {'dataset':'roman_coins'}