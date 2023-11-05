import pandas as pd
from fastapi import FastAPI

app = FastAPI()

df = pd.read_csv('web_scraping/roman_coins.csv')

