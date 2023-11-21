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
    year: str = None,
    min_year: str = None,
    max_year: str = None,
    mass: str = None,
    min_mass: str = None,
    max_mass: str = None,
    diameter: str = None,
    min_diameter: str = None,
    max_diameter: str = None
    ):

    cur = db.cursor()

    # Base query
    query = 'SELECT * FROM roman_coins'

    # Mapping filters to their SQL query equivalents
    filter_mappings = {
        'ruler': ('ruler', '=', ruler),
        'metal': ('metal', '=', metal),
        'era': ('era', '=', era),
        'year': ('year', '=', year),
        'min_year':('year', '>=', min_year),
        'max_year':('year', '<=', max_year),
        'mass': ('mass', '=', mass),
        'min_mass':('mass', '>=', min_mass),
        'max_mass':('mass', '<=', max_mass),
        'diameter': ('diameter', '=', diameter),
        'min_diameter':('diameter', '>=', min_diameter),
        'max_diameter':('diameter', '<=', max_diameter)
    }

    # Filtering logic
    try:
        filter_clauses = [(f'{col} {op} ?', val) for col, op, val in filter_mappings.values() if val is not None]
        conditions, params = [list(a) for a in zip(*filter_clauses)]
        query += ' WHERE ' + ' AND '.join(conditions)
    except:
        params = []
    
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
    query: Annotated[list[str], Query(min_length=1, max_length=50)] = None, 
    db: sqlite3.Connection = Depends(get_db)
    ):

    if query:
        cur = db.cursor()
        sql = 'SELECT * FROM roman_coins'
        search_items = [items if len(items) > 1  else query for items in query]
        params = [f'%{item}%' for item in search_items]
        filters = ['description LIKE ?' for _ in search_items]
        sql += ' WHERE ' + ' AND '.join(filters)
        cur.execute(sql, params)
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
    