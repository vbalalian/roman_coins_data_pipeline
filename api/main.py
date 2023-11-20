from fastapi import FastAPI, Query, HTTPException, Depends
from typing import Annotated
import sqlite3

db_path = 'api/roman_coins.db'

app = FastAPI()

def get_db():
    db = sqlite3.connect(db_path)
    db.row_factory = sqlite3.Row
    try:
        yield db
    finally:
        db.close()

@app.get('/')
def root():
    return {
        'message':'Welcome to the Roman Coin Data API',
        'status':'OK',
        'available_versions':['/v1/'],
        'documentation':'/docs'
        }

@app.get('/v1/')
def v1_root():
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
def read_coins(
    db: sqlite3.Connection = Depends(get_db), 
    page: int = 1, 
    page_size: int = 10, 
    sort_by: str = None,
    ruler: str = None,
    metal: str = None,
    era: str = None,
    year: int = None
    ):

    cur = db.cursor()

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

    return [dict(row) for row in coins]

@app.get('/v1/coins/search')
def search_coins(
    query: Annotated[str | None, Query(min_length=1, max_length=50)], 
    db: sqlite3.Connection = Depends(get_db)
    ):

    if query:
        cur = db.cursor()
        cur.execute('SELECT * FROM roman_coins WHERE description LIKE ?', ('%' + query + '%',))
        search_result = cur.fetchall()
        return [dict(row) for row in search_result]
    
    raise HTTPException(status_code=400, detail='Query string is empty')

@app.get('/v1/coins/id/{coin_id}')
def coin_by_id(coin_id: str, db: sqlite3.Connection = Depends(get_db)):

    cur = db.cursor()
    cur.execute('SELECT * FROM roman_coins WHERE id = ?', (coin_id,))
    coin = cur.fetchone()
    if coin:
        return dict(coin)
    
    raise HTTPException(status_code=404, detail='Coin not found')
    