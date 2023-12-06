from fastapi import FastAPI, Query, HTTPException, Depends
from pydantic import BaseModel, confloat, conint, validator
from typing import Annotated
import psycopg2
from psycopg2.extras import RealDictCursor
import os

# Path to roman_coins database
db_name = os.getenv('DB_NAME')
db_user = os.getenv('DB_USER')
db_password = os.getenv('DB_PASSWORD')
db_host = 'db'

app = FastAPI()

# Database connection manager
def get_conn():
    conn = psycopg2.connect(
        dbname=db_name, 
        user=db_user, 
        password=db_password, 
        host=db_host, 
        cursor_factory=RealDictCursor)
    try:
        yield conn
    finally:
        conn.close()

# Base root
@app.get('/')
async def root():
    return {
        'message':'Welcome to the Roman Coin Data API',
        'status':'OK',
        'available_versions':['/v1/'],
        'documentation':'/docs'
        }

# Version 1 root
@app.get('/v1/')
async def v1_root():
    return {
        'message': 'Welcome to the Roman Coin Data API - Version 1',
        'status': 'OK',
        'documentation': '/docs/',
        'endpoints': {
            '/v1/coins': 'Retrieve a paginated list of coins with optional sorting and filtering. You can filter by properties like ruler, metal, era, and more, as well as sort the results.',
            '/v1/coins/id/{coin_id}': 'Retrieve detailed information about a single coin by its ID. This endpoint provides complete data about a specific coin.',
            '/v1/coins/search': 'Search for coins based on a query. This allows you to find coins by matching against their descriptions or other text attributes.',
            '/v1/coins/id/{coin_id} [POST]': 'Add a new coin to the database. This endpoint is for inserting new coin data into the collection.',
            '/v1/coins/id/{coin_id} [PUT]': 'Fully update an existing coin’s data. This endpoint replaces all data for the specified coin. Unspecified fields in the request are set to their default values or null.',
            '/v1/coins/id/{coin_id} [PATCH]': 'Partially update an existing coin’s data. Use this endpoint to modify specific fields without affecting the rest of the coin’s data.'
        }
    }

def validate_sort_column(sort_by:str):
    '''Validate the sort_by parameter to ensure it's a valid column name'''
    allowed_sort_columns = ['ruler', 'metal', 'year', 'mass', 'diameter']
    if sort_by not in allowed_sort_columns:
        raise HTTPException(status_code=400, detail='Invalid sort column')
    return sort_by

# Endpoint for all coins, with sorting and filtering
@app.get('/v1/coins/')
async def read_coins(
    db: psycopg2.extensions.connection = Depends(get_conn), 
    page: int = 1, 
    page_size: int = 10, 
    sort_by: str = None,
    desc: bool = False,
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
        filter_clauses = [(f'{col} {op} %s', val) for col, op, val in filter_mappings.values() if val is not None]
        conditions, params = [list(a) for a in zip(*filter_clauses)]
        query += ' WHERE ' + ' AND '.join(conditions)
    except:
        params = []
    
    # Sorting logic
    if sort_by:
        sort_by = validate_sort_column(sort_by)
        if desc == True:
            sort_by += ' DESC'
        query += f' ORDER BY {sort_by}'

    # Pagination logic
    query += ' LIMIT %s OFFSET %s'
    params += [page_size, (page - 1) * page_size]

    # Execute query
    try:
        cur = db.cursor()
        cur.execute(query, params)
        coins = cur.fetchall()
    except psycopg2.Error as e:
        print('Read error:', e)
    finally:
        cur.close()
    if coins:
        return [dict(row) for row in coins]
    raise HTTPException(status_code=400, detail='No matching coins found')

# Coin Search endpoint
@app.get('/v1/coins/search')
async def search_coins(
    query: Annotated[list[str], Query(min_length=1, max_length=50)] = None, 
    db: psycopg2.extensions.connection = Depends(get_conn)
    ):

    if query:

        try:
            cur = db.cursor()
            sql = 'SELECT * FROM roman_coins'
            search_items = [items if len(items) > 1  else query for items in query]
            params = [f'%{item}%' for item in search_items]
            filters = ['description LIKE %s' for _ in search_items]
            sql += ' WHERE ' + ' AND '.join(filters)
            cur.execute(sql, params)
            search_result = cur.fetchall()
        except psycopg2.Error as e:
            print('Search error:', e)
        finally:
            cur.close()

        return [dict(row) for row in search_result]
    
    raise HTTPException(status_code=400, detail='Query string is empty')

# Coins by ID endpoint
@app.get('/v1/coins/id/{coin_id}')
async def coin_by_id(coin_id: str, db: psycopg2.extensions.connection = Depends(get_conn)):

    try:
        cur = db.cursor()

        # Execute SQL to fetch matching coin
        cur.execute('SELECT * FROM roman_coins WHERE id = %s', (coin_id,))
        coin = cur.fetchone()
        if coin:
            return dict(coin)
    except psycopg2.Error as e:
        print('ID error:', e)
    finally:
        cur.close()
    
    raise HTTPException(status_code=404, detail='Coin not found')

# Coin validation model 
class Coin(BaseModel):
    ruler: str | None = None
    ruler_detail: str | None = None
    description: str | None = None
    metal: str | None = None
    mass: confloat(ge=0, le=50) = 0.0
    diameter: confloat(ge=0, le=50) = 0.0
    era: str = None
    year: conint(ge=-50, le=500) = None
    inscriptions: str = None
    txt: str = None

    # Additional metal validation
    @validator('metal')
    def validate_metal(cls, v):
        valid_metals = ['Gold', 'Silver', 'Copper', 'Bronze', 'Lead', 'Bone', 'FAKE']
        if v.title() not in valid_metals:
            raise ValueError('Invalid metal')
        return v.title()
    
    # Additional era validation
    @validator('era')
    def validate_era(cls, v):
        valid_eras = ['BC', 'AD']
        if v.upper() not in valid_eras:
            raise ValueError('Invalid era')
        return v.upper()
    
# Add coin endpoint
@app.post('/v1/coins/id/{coin_id}', status_code=201)
async def add_coin(coin_id:str, coin:Coin, db: psycopg2.extensions.connection = Depends(get_conn)):

    # SQL for adding a Coin to the database
    insert_query = '''
    INSERT INTO roman_coins (ruler, ruler_detail, id, description, metal, mass, diameter, era, year, inscriptions, txt)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    '''

    # Data to be inserted
    coin_data = (
        coin.ruler,
        coin.ruler_detail,
        coin_id,
        coin.description,
        coin.metal,
        coin.mass,
        coin.diameter,
        coin.era,
        coin.year,
        coin.inscriptions,
        coin.txt
    )

    # Execute the query
    try:
        cur = db.cursor()
        cur.execute(insert_query, coin_data)
        db.commit()
    except psycopg2.Error as e:
        db.rollback()
        raise HTTPException(status_code=400, detail=f"Error inserting coin: {e}")
    finally:
        cur.close()

    return {"message": "Coin added successfully"}

# Full coin update endpoint
@app.put("/v1/coins/id/{coin_id}", status_code=200)
async def update_coin(coin_id: str, coin_update: Coin, db: psycopg2.extensions.connection = Depends(get_conn)):
    '''Updates entire row. Missing fields will reset to default values.'''
    update_fields = coin_update.dict()
    if not update_fields:
        raise HTTPException(status_code=400, detail="No fields to update")

    set_clause = ", ".join([f"{key} = %s" for key in update_fields.keys()])
    values = list(update_fields.values())

    update_query = f"UPDATE roman_coins SET {set_clause} WHERE id = %s"
    values.append(coin_id)

    try:
        cur = db.cursor()
        cur.execute(update_query, values)
        db.commit()
    except psycopg2.Error as e:
        db.rollback()
        raise HTTPException(status_code=400, detail=f"Error updating coin: {e}")
    finally:
        cur.close()

    return {"message": "Coin updated successfully"}

# Partial coin update endpoint
@app.patch("/v1/coins/id/{coin_id}", status_code=200)
async def patch_coin(coin_id: str, coin_update: Coin, db: psycopg2.extensions.connection = Depends(get_conn)):
    update_fields = coin_update.dict(exclude_unset=True)
    if not update_fields:
        raise HTTPException(status_code=400, detail="No fields to update")

    set_clause = ", ".join([f"{key} = %s" for key in update_fields.keys()])
    values = list(update_fields.values())

    update_query = f"UPDATE roman_coins SET {set_clause} WHERE id = %s"
    values.append(coin_id)

    try:
        cur = db.cursor()
        cur.execute(update_query, values)
        if cur.rowcount == 0:
            raise HTTPException(status_code=404, detail="Coin not found")
        db.commit()
    except psycopg2.Error as e:
        db.rollback()
        raise HTTPException(status_code=400, detail=f"Error updating coin: {e}")
    finally:
        cur.close()

    return {"message": "Coin updated successfully"}
