from fastapi import FastAPI, Query, Path, HTTPException, Depends
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field, field_validator
from typing import Annotated
import psycopg2
from psycopg2.extras import RealDictCursor
import os
from datetime import datetime

app = FastAPI()

# Database connection manager
def get_conn():
    conn = psycopg2.connect(
        dbname=os.getenv('DB_NAME'), 
        user=os.getenv('DB_USER'), 
        password=os.getenv('DB_PASSWORD'), 
        host='db', 
        cursor_factory=RealDictCursor)
    try:
        yield conn
    finally:
        conn.close()

# Base root
@app.get('/')
async def root() -> JSONResponse:
    return JSONResponse(content={
        'message':'Welcome to the Roman Coin Data API',
        'status':'OK',
        'available_versions':['/v1/'],
        'documentation':'/docs'
        })

# Version 1 root
@app.get('/v1/')
async def v1_root() -> JSONResponse:
    return JSONResponse(content={
        'message': 'Welcome to the Roman Coin Data API - Version 1',
        'status': 'OK',
        'documentation': '/docs/',
        'endpoints': {
            '/v1/coins': 'Retrieve a paginated list of coins with optional sorting and filtering. You can filter by properties like name, metal, era, and more, as well as sort the results.',
            '/v1/coins/id/{coin_id}': 'Retrieve detailed information about a single coin by its ID. This endpoint provides complete data about a specific coin.',
            '/v1/coins/search': 'Search for coins based on a query. This allows you to find coins by matching against their descriptions or other text attributes.',
            '/v1/coins/id/{coin_id} [POST]': 'Add a new coin to the database. This endpoint is for inserting new coin data into the collection.',
            '/v1/coins/id/{coin_id} [PUT]': 'Fully update an existing coin’s data. This endpoint replaces all data for the specified coin. Unspecified fields in the request are set to their default values or null.',
            '/v1/coins/id/{coin_id} [PATCH]': 'Partially update an existing coin’s data. Use this endpoint to modify specific fields without affecting the rest of the coin’s data.'
        }
    })

# Coin validation models
class CoinDetails(BaseModel):
    name: str | None = Field(default=None, title="The name of the coin's figurehead", max_length=30)
    name_detail: str | None = Field(default=None, title="The subtitle/detail of the coin's figurehead", max_length=1000)
    catalog: str | None = Field(default=None, title="The catalog info associated with the coin", max_length=80)
    description: str | None = Field(default=None, title="The description of the coin", max_length=1000)
    metal: str | None = Field(default=None, title="The metal/material composition of the coin", max_length=20)
    mass: float | None = Field(gt=0.0, lt=50, default=None, title="The mass of the coin in grams")
    diameter: float | None = Field(gt=0.0, le=50, default=None, title="The diameter of the coin in millimeters")
    era: str | None = Field(default=None, title="The era of the coin e.g. BC or AD")
    year: int | None = Field(ge=-750, le=500, default=None, title="The year associated with the coin")
    inscriptions: str | None = Field(default=None, title="Recognized inscriptions found on the coin")
    txt: str | None = Field(default=None, title="Filename of alternate coin information .txt")

    # Additional metal validation
    @field_validator('metal')
    def validate_metal(cls, v):
        valid_metals = ['Gold', 'Silver', 'Copper', 'Bronze', 'Lead', 'Bone', 'Fake']
        if v:
            if v.title() not in valid_metals:
                raise ValueError('Invalid metal')
            return v.title()
    
    # Additional era validation
    @field_validator('era')
    def validate_era(cls, v):
        valid_eras = ['BC', 'AD']
        if v:
            if v.upper() not in valid_eras:
                raise ValueError('Invalid era')
            return v.upper()

    model_config = {
        "json_schema_extra": {
            "examples": [
                {
                    "name": "Augustus",
                    "name_detail": "The first Roman Emperor, aka Octavian, adopted son of Julius Caesar",
                    "description": "Denarius, Victory crowning an eagle / Laureate head right",
                    "catalog": "RIC 248",
                    "metal": "Silver",
                    "mass": 8.1,
                    "diameter": 10.8, 
                    "era": "AD", 
                    "year": 24,
                    "inscriptions": "AVG,CAES,PON",
                    "txt": "RIC_248.txt"
                }
            ]
        }
    }

class Coin(CoinDetails):
    id: str = Field(title="The coin's ID", min_length=10, max_length=50)
    created: datetime | None = Field(default=None, title="Date and Time the row was created")
    modified: datetime | None = Field(default=None, title="Date and Time the row was modified")
    model_config = {
        "json_schema_extra": {
            "examples": [
                {
                    "id": "f351566b-7f7b-4ff6-9d90-aac9a09045db",
                    "name": "Augustus",
                    "name_detail": "The first Roman Emperor, aka Octavian, adopted son of Julius Caesar",
                    "catalog": "RIC 248",
                    "description": "Denarius, Victory crowning an eagle / Laureate head right",
                    "metal": "Silver",
                    "mass": 8.1,
                    "diameter": 10.8, 
                    "era": "AD", 
                    "year": 24,
                    "inscriptions": "AVG,CAES,PON",
                    "txt": "RIC_248.txt",
                    "created": "2023-12-08 14:33:06.176375",
                    "modified": "2023-12-08 14:34:06.176375"
                }
            ]
        }
    }

# Pagination models
class Pagination(BaseModel):
    total_items: int
    total_pages: int
    current_page: int
    items_per_page: int

class PaginatedResponse(BaseModel):
    data: list[Coin]
    pagination: Pagination

def validate_sort_column(sort_by:str):
    '''Validate the sort_by parameter to ensure it's a valid column name'''
    allowed_sort_columns = ['name', 'catalog', 'metal', 'year', 'mass', 'diameter', 'created', 'modified']
    if sort_by.lower() not in allowed_sort_columns:
        raise HTTPException(status_code=400, detail='Invalid sort column')
    return sort_by

# Endpoint for all coins, with sorting and filtering
@app.get('/v1/coins/', response_model=PaginatedResponse, response_model_exclude_none=True)
async def read_coins(
    db: psycopg2.extensions.connection = Depends(get_conn), 
    page: int = 1, 
    page_size: int = 10, 
    sort_by: str = None,
    desc: bool = False,
    name: str = None,
    metal: str = None,
    era: str = None,
    year: str = None,
    min_year: str = None,
    max_year: str = None,
    min_mass: str = None,
    max_mass: str = None,
    min_diameter: str = None,
    max_diameter: str = None,
    start_created: datetime = None,
    end_created: datetime = None,
    start_modified: datetime = None,
    end_modified: datetime = None
    ):

    if name:
        name = name.title()
    if metal:
        metal = metal.title()
    if era:
        era = era.upper()
    # Base query
    query = 'SELECT * FROM roman_coins'

    # Mapping filters to their SQL query equivalents
    filter_mappings = {
        'name': ('name', '=', name),
        'metal': ('metal', '=', metal),
        'era': ('era', '=', era),
        'year': ('year', '=', year),
        'min_year':('year', '>=', min_year),
        'max_year':('year', '<=', max_year),
        'min_mass':('mass', '>=', min_mass),
        'max_mass':('mass', '<=', max_mass),
        'min_diameter':('diameter', '>=', min_diameter),
        'max_diameter':('diameter', '<=', max_diameter),
        'start_created':('created', '>=', start_created),
        'end_created':('created', '<=', end_created),
        'start_modified':('modified', '>=', start_modified),
        'end_modified':('modified', '<=', end_modified)
    }


    # Filtering logic
    try:
        filter_clauses = [(f'{col} {op} %s', val) for col, op, val in filter_mappings.values() if val is not None]
        conditions, params = [list(a) for a in zip(*filter_clauses)]
        query += ' WHERE ' + ' AND '.join(conditions)
    except:
        conditions, params = [], []
    
    # Sorting logic
    if sort_by:
        sort_by = validate_sort_column(sort_by)
        if desc == True:
            sort_by += ' DESC'
        query += f' ORDER BY {sort_by}'

    try:
        with db.cursor() as cur:
            # Count total items
            count_query = 'SELECT COUNT(*) FROM roman_coins'
            if conditions:
                count_query += ' WHERE ' + ' AND '.join(conditions)

            cur.execute(count_query, params)
            total_items = cur.fetchone()['count']

            # Pagination logic
            query += ' LIMIT %s OFFSET %s'
            params += [page_size, (page - 1) * page_size]

            # Execute main query
            cur.execute(query, params)
            coins = cur.fetchall()
        
        # Calculate pagination metadata
        total_pages = total_items // page_size + (total_items % page_size > 0)
        pagination = Pagination(
            total_items=total_items,
            total_pages=total_pages,
            current_page=page,
            items_per_page=page_size
        )

        if coins:
            return PaginatedResponse(
                data = [dict(row) for row in coins],
                pagination=pagination
            )
        else:
            raise HTTPException(status_code=400, detail='No matching coins found')
            
    except psycopg2.Error as e:
        print('Database error:', e)
        raise HTTPException(status_code=500, detail='Internal Server Error')

# Coin Search endpoint
@app.get('/v1/coins/search', response_model=list[Coin], response_model_exclude_none=True)
async def search_coins(
    query: Annotated[str, Query(title='Query string', min_length=3, max_length=50, examples=["crowned by Victory"])], 
    db: psycopg2.extensions.connection = Depends(get_conn)
    ) -> list[Coin]:

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
    if search_result:
        return [dict(row) for row in search_result]
    else:
        raise HTTPException(status_code=400, detail='No matching coins found')

# Coins by ID endpoint
@app.get('/v1/coins/id/{coin_id}', response_model=Coin, response_model_exclude_none=True)
async def coin_by_id(
    coin_id: Annotated[str, Path(title='The ID of the coin to be retrieved', 
                                 examples=["64c3075e-2b01-4b09-a4f0-07be61f7f9b7"],
                                 min_length=10, max_length=50)], 
    db: psycopg2.extensions.connection = Depends(get_conn)
    ) -> Coin:

    try:
        cur = db.cursor()
        cur.execute('SELECT * FROM roman_coins WHERE id = %s', (coin_id,))
        coin = cur.fetchone()
        if coin:
            return dict(coin)
    except psycopg2.Error as e:
        print('ID error:', e)
        raise HTTPException(status_code=500, detail='Internal Server Error')
    finally:
        cur.close()
    
    raise HTTPException(status_code=404, detail='Coin not found')
    
# Add coin endpoint
@app.post('/v1/coins/id/{coin_id}')
async def add_coin(
    coin_id:Annotated[str, Path(title='The ID of the coin to be added')], 
    coin_details:CoinDetails, 
    db: psycopg2.extensions.connection = Depends(get_conn)
    ) -> JSONResponse:

    # SQL for adding a Coin to the database
    insert_query = '''
    INSERT INTO roman_coins (id, name, name_detail, catalog, description, metal, mass, diameter, era, year, inscriptions, txt, created, modified)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    '''

    current_datetime = datetime.now()

    # Data to be inserted
    coin_data = (
        coin_id,
        coin_details.name,
        coin_details.name_detail,
        coin_details.catalog,
        coin_details.description,
        coin_details.metal,
        coin_details.mass,
        coin_details.diameter,
        coin_details.era,
        coin_details.year,
        coin_details.inscriptions,
        coin_details.txt,
        current_datetime,
        current_datetime
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

    return JSONResponse(status_code=201, content={"message": "Coin added successfully"})

# Full coin update endpoint
@app.put("/v1/coins/id/{coin_id}", status_code=200)
async def update_coin(
    coin_id: Annotated[str, Path(title='The ID of the coin to be updated')], 
    coin_update: CoinDetails, 
    db: psycopg2.extensions.connection = Depends(get_conn)
    ) -> JSONResponse:
    '''Updates entire row. Missing fields will reset to default values.'''
    update_fields = coin_update.model_dump()
    if not update_fields:
        raise HTTPException(status_code=400, detail="No fields to update")
    update_fields["modified"] = datetime.now()

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

    return JSONResponse(content={"message": "Coin updated successfully"})

# Partial coin update endpoint
@app.patch("/v1/coins/id/{coin_id}")
async def patch_coin(
    coin_id: Annotated[str, Path(title='The ID of the coin to be updated')], 
    coin_update: CoinDetails, 
    db: psycopg2.extensions.connection = Depends(get_conn)
    ) -> JSONResponse:
    update_fields = coin_update.model_dump(exclude_unset=True)
    if not update_fields:
        raise HTTPException(status_code=400, detail="No fields to update")
    update_fields["modified"] = datetime.now()

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

    return JSONResponse(status_code=200, content={"message": "Coin updated successfully"})
