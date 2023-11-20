from fastapi import FastAPI, Query, HTTPException
from typing import Annotated
import sqlite3

db_path = 'api/roman_coins.db'

app = FastAPI()

def connect_db():
    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row
    return con

@app.get('/')
async def root():
    return {
        'message':'Welcome to the Roman Coin Data API',
        'status':'OK',
        'available_versions':['/v1/'],
        'documentation':'/docs'
        }

@app.get('/v1/')
async def v1_root():
    return {
        'message':'Welcome to the Roman Coin Data API - Version 1',
        'status':'OK',
        'documentation':'/docs/',
        'endpoints': {
            '/v1/coins': 'Retrieve a paginated list of coins with optional sorting and filtering',
            '/v1/coins/id/{coin_id}': 'Retrieve detailed information about a single coin by its ID',
            '/v1/coins/search': 'Search for coins based on query'
            }
        }

@app.get('/v1/coins/')
async def read_coins(
    page: int = 1, 
    page_size: int = 10, 
    sort_by: str = None,
    ruler: str = None,
    metal: str = None,
    era: str = None,
    year: int = None
    ):

    with connect_db() as con:
        cur = con.cursor()

        # Base query
        query = 'SELECT * FROM roman_coins'
        params = []

        # Filtering logic
        conditions = []
        if ruler:
            conditions.append('ruler = ?')
            params.append(ruler)
        if metal:
            conditions.append('metal = ?')
            params.append(metal)
        if era:
            conditions.append('era = ?')
            params.append(era)
        if year:
            conditions.append('year = ?')
            params.append(year)
        if conditions:
            query += ' WHERE ' + ' AND '.join(conditions)
        
        # Sorting logic
        if sort_by:
            query += f' ORDER BY {sort_by}'

        # Pagination logic
        query += ' LIMIT ? OFFSET ?'
        params += [page_size, (page - 1) * page_size]

        # Execute query
        cur.execute(query, params)
        coins = cur.fetchall()

    con.close()
    return [dict(row) for row in coins]

@app.get('/v1/coins/search')
async def search_coins(query: Annotated[str | None, Query(min_length=1, max_length=50)]):

    if query:
        with connect_db() as con:
            cur = con.cursor()
            cur.execute('SELECT * FROM roman_coins WHERE description LIKE ?', ('%' + query + '%',))
            search_result = cur.fetchall()
        con.close()
        return [dict(row) for row in search_result]
    
    raise HTTPException(status_code=400, detail='Query string is empty')

@app.get('/v1/coins/id/{coin_id}')
async def coin_by_id(coin_id: str):

    with sqlite3.connect(db_path) as con:
        cur = con.cursor()
        cur.execute('SELECT * FROM roman_coins WHERE id = ?', (coin_id,))
        coin = cur.fetchone()
    con.close()
    if coin is None:
        raise HTTPException(status_code=404, detail='Coin not found')
    return dict(coin)