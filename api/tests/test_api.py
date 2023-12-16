import os
import sys
sys.path.append(os.getcwd()) # Add cwd to path
from fastapi.testclient import TestClient
from main import app, get_conn
import pytest
import psycopg2
from psycopg2.extras import RealDictCursor
import json

@pytest.fixture(scope='module')
def test_client():
    with TestClient(app) as client:
        yield client

@pytest.fixture(scope='module')
def test_database():

    test_db = "test_database"
    test_user = "postgres"
    test_password = "postgres"
    test_host = "test_db"

    columns = ['id', 'ruler', 'ruler_detail', 'catalog', 'description', 
                    'metal', 'mass', 'diameter', 'era', 'year', 'inscriptions', 
                    'txt', 'created', 'modified']
    dtypes = [
        'VARCHAR(50) PRIMARY KEY', 'VARCHAR(30)', 'VARCHAR(1000)', 'VARCHAR(80)', 'VARCHAR(1000)', 
        'VARCHAR(20)', 'REAL', 'REAL', 'VARCHAR(5)', 'INTEGER', 'VARCHAR(100)', 
        'VARCHAR(105)', 'TIMESTAMP', 'TIMESTAMP'
        ]

    def get_test_conn():
        '''Returns a connection with the test Postgres database'''
        conn = psycopg2.connect(
            dbname=test_db,
            user=test_user,
            password=test_password,
            host=test_host,
            cursor_factory=RealDictCursor
        )
        try:
            yield conn
        finally:
            conn.close()

    def create_test_table(conn:psycopg2.extensions.connection, cols:list, dtypes:list):
        '''Creates a table in the test database'''
        with conn.cursor() as cur:
            cur.execute(f'CREATE TABLE IF NOT EXISTS roman_coins (' + 
                        ', '.join(f'{col} {dtype}' for col, dtype
                                  in zip(cols, dtypes)) + ');')
    
    def insert_test_data(conn:psycopg2.extensions.connection, coins:json):
        '''Loads sample data into test table'''
        with conn.cursor() as cur:
            for coin in coins:
                columns = ', '.join(coin.keys())
                placeholders = ', '.join(f'%({col})s' for col in coin.keys())
                query = f'INSERT INTO roman_coins ({columns}) VALUES ({placeholders});'
                cur.execute(query, coin)
                conn.commit()

    def teardown_test_data(conn:psycopg2.extensions.connection):
        with conn.cursor() as cur:
            cur.execute("DROP TABLE roman_coins;")
            conn.commit()
        conn.close()

    app.dependency_overrides[get_conn] = get_test_conn
    
    conn =  psycopg2.connect(
            dbname=test_db,
            user=test_user,
            password=test_password,
            host=test_host,
            cursor_factory=RealDictCursor
        )
    try:
        create_test_table(conn, columns, dtypes)
        with open("tests/test_data/test_coins.json") as file:
            test_coins_data = json.load(file)
        insert_test_data(conn, test_coins_data)
        yield
    finally:
        teardown_test_data(conn)
        app.dependency_overrides.clear()

# Base root
def test_root(test_client):
    response = test_client.get("/")
    assert response.status_code == 200
    assert response.json() == {
        "message":"Welcome to the Roman Coin Data API",
        "status":"OK",
        "available_versions":["/v1/"],
        "documentation":"/docs"
    }

# Version 1 root
def test_v1_root(test_client):
    response = test_client.get("/v1/")
    assert response.status_code == 200
    assert response.json()["message"] == "Welcome to the Roman Coin Data API - Version 1"
    assert response.json()["status"] == "OK"

# Endpoint for all coins, with sorting and filtering
def test_read_coins(test_client, test_database):
    
    # Default parameters
    default_response = test_client.get("/v1/coins/")
    assert default_response.status_code == 200
    assert len(default_response.json()["data"]) == 10
    assert default_response.json()["pagination"] == {"total_items":20, "total_pages":2, "current_page":1, "items_per_page":10}
    
    # Pagination
    page_response = test_client.get("/v1/coins/?page=2&page_size=8")
    assert page_response.status_code == 200
    assert len(page_response.json()["data"]) == 8
    assert page_response.json()["pagination"] == {"total_items":20, "total_pages":3, "current_page":2, "items_per_page":8}
    
    # Sorting
    sort_response_1 = test_client.get("/v1/coins/?sort_by=ruler&desc=True")
    assert sort_response_1.status_code == 200
    assert len(sort_response_1.json()["data"]) == 10
    assert sort_response_1.json()["data"][0]["ruler"] == "Hadrian"

    sort_response_2 = test_client.get("/v1/coins/?sort_by=year")
    assert sort_response_2.status_code == 200
    assert len(sort_response_2.json()["data"]) == 10
    assert sort_response_2.json()["data"][0]["year"] == 378

    sort_response_3 = test_client.get("/v1/coins/?sort_by=description")
    assert sort_response_3.status_code == 400

    # Filtering
    filter_response_1 = test_client.get(r"/v1/coins/?ruler=aelia%20Flaccilla&min_diameter=21")
    assert filter_response_1.status_code == 200
    assert len(filter_response_1.json()["data"]) == 5

    filter_response_2 = test_client.get(r"/v1/coins/?metal=gold&max_mass=2")
    assert filter_response_2.status_code == 200
    assert len(filter_response_2.json()["data"]) == 3
    assert filter_response_2.json()["data"][0]["metal"] == "Gold"

    filter_response_3 = test_client.get(r"/v1/coins/?min_year=100&max_year=400")
    assert filter_response_3.status_code == 200
    assert len(filter_response_3.json()["data"]) == 5

    filter_response_4 = test_client.get(r"/v1/coins/?era=ad")
    assert filter_response_4.status_code == 200
    assert len(filter_response_4.json()["data"]) == 9
    assert filter_response_4.json()["data"][0]["era"] == "AD"

    filter_response_5 = test_client.get(r"/v1/coins/?start_created=2023-12-11T07:55:00")
    assert filter_response_5.status_code == 200
    assert len(filter_response_5.json()["data"]) == 1

    filter_response_6 = test_client.get(r"/v1/coins/?min_diameter=30")
    assert filter_response_6.status_code == 400

    