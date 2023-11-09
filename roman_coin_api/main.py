from fastapi import FastAPI, Query, HTTPException
import sqlite3

db_path = '.sqlite3/roman_coins.db'

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
        'documentation':'/docs',
        'endpoints': {
            '/coins': 'Retrieve a paginated list of coins with optional sorting and filtering',
            '/coins/id/{coin_id}': 'Retrieve detailed information about a single coin by its ID',
            '/coins/search': 'Search for coins based on query'
            }
        }

@app.get('/coins/')
async def read_coins(
    page: int = 1, 
    page_size: int = 10, 
    sort_by: str = None,
    ruler: str = None,
    metal: str = None,
    era: str = None,
    year: str = None
    ):

    con = connect_db()
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

    cur.execute(query, params)
    coins = cur.fetchall()

    con.close()

    return [dict(row) for row in coins]

@app.get('/coins/search')
async def search_coins(query: str | None = Query(default=None, max_length=50)):
    con = connect_db()
    cur = con.cursor()

    if query:
        search_result = df[df['description'].str.contains(query, na=False)]
        return search_result.to_dict(orient='records')
    return {'error': 'Query string is empty'}

    con.close()

@app.get('/coins/id/{coin_id}')
async def read_coin(coin_id: str):
    coin = df[df['id'] == coin_id]
    if coin.empty:
        return {'error': 'Coin not found'}
    return coin.iloc[0].to_dict()