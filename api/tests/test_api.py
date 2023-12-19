import os
import sys
sys.path.append(os.getcwd()) # Add cwd to path
from fastapi.testclient import TestClient
from main import app, get_conn
import pytest
import psycopg2
from psycopg2.extras import RealDictCursor
import json
from datetime import datetime

# Set up test client
@pytest.fixture(scope='module')
def test_client():
    with TestClient(app) as client:
        yield client

# Set up test database
@pytest.fixture(scope='module')
def test_database():

    test_db = "test_database"
    test_user = "postgres"
    test_password = "postgres"
    test_host = "test_db"

    columns = ['id', 'name', 'name_detail', 'catalog', 'description', 
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
    response = test_client.get("/v1/coins/")
    assert response.status_code == 200
    assert len(response.json()["data"]) == 10
    assert response.json()["pagination"] == {"total_items":20, "total_pages":2, "current_page":1, "items_per_page":10}
    
    # Pagination
    response = test_client.get("/v1/coins/?page=2&page_size=8")
    assert response.status_code == 200
    assert len(response.json()["data"]) == 8
    assert response.json()["pagination"] == {"total_items":20, "total_pages":3, "current_page":2, "items_per_page":8}
    
    # Sorting
    response = test_client.get("/v1/coins/?sort_by=name&desc=True")
    assert response.status_code == 200
    assert len(response.json()["data"]) == 10
    assert response.json()["data"][0]["name"] == "Hadrian"

    response = test_client.get("/v1/coins/?sort_by=year")
    assert response.status_code == 200
    assert len(response.json()["data"]) == 10
    assert response.json()["data"][0]["year"] == 378

    response = test_client.get("/v1/coins/?sort_by=mass")
    assert response.status_code == 200
    assert response.json()["data"][0]["mass"] == 1.46

    response = test_client.get("/v1/coins/?sort_by=description")
    assert response.status_code == 400

    # Filtering
    response = test_client.get(r"/v1/coins/?name=aelia%20Flaccilla&min_diameter=21")
    assert response.status_code == 200
    assert len(response.json()["data"]) == 5

    response = test_client.get(r"/v1/coins/?metal=gold&max_mass=2")
    assert response.status_code == 200
    assert len(response.json()["data"]) == 3
    assert response.json()["data"][0]["metal"] == "Gold"

    response = test_client.get(r"/v1/coins/?min_year=100&max_year=400")
    assert response.status_code == 200
    assert len(response.json()["data"]) == 5

    response = test_client.get(r"/v1/coins/?era=ad")
    assert response.status_code == 200
    assert len(response.json()["data"]) == 9
    assert response.json()["data"][0]["era"] == "AD"

    response = test_client.get(r"/v1/coins/?start_created=2023-12-11T07:55:00")
    assert response.status_code == 200
    assert len(response.json()["data"]) == 1

    response = test_client.get(r"/v1/coins/?min_diameter=30")
    assert response.status_code == 400

# Coin Search endpoint
def test_search_coins(test_client, test_database):
    
    # Query with multiple results
    response = test_client.get(r"/v1/coins/search?query=Constantinople")
    assert response.status_code ==200
    assert len(response.json()) == 8

    # Query with one result
    response = test_client.get(r"/v1/coins/search?query=Hadrian")
    assert response.status_code ==200
    assert len(response.json()) == 1
    assert response.json()[0]["name"] == "Hadrian"

    # Query with no results
    response = test_client.get(r"/v1/coins/search?query=Caligula")
    assert response.status_code == 400
    assert response.json()["detail"]== "No matching coins found"

    # Empty query
    response = test_client.get(r"/v1/coins/search?query=")
    assert response.status_code == 422
    assert response.json()["detail"][0]["type"] == "string_too_short"

    # Multiple queries
    response = test_client.get(r"/v1/coins/search?query=Victory&query=officina")
    assert response.status_code == 200
    assert len(response.json()) == 2

# Coins by ID endpoint
def test_coin_by_id(test_client, test_database):

    # Valid ID
    response = test_client.get(r"/v1/coins/id/343a3001-ae2e-4745-888e-994374e398a3")
    assert response.status_code ==200
    assert response.json()["name"] == "Aelia Ariadne"
    
    # Non-existent ID
    response = test_client.get(r"/v1/coins/id/023-450938fgldf-to0r90ftu-438537")
    assert response.status_code == 404
    assert response.json()["detail"] == "Coin not found"

    # Invalid ID
    response = test_client.get(r"/v1/coins/id/023450938")
    assert response.status_code == 422
    assert response.json()["detail"][0]["type"] == "string_too_short"

    # Empty ID
    response = test_client.get(r"/v1/coins/id/")
    assert response.status_code == 404

# Add coin endpoint
def test_add_coin(test_client, test_database):

    # Normal case, no missing fields
    coin = {"name":"Test name 1", "name_detail":"Test name Detail", 
            "catalog":"Test Catalog", 
            "description":"This is a test description, minimum 50 characters. ",
            "metal":"Gold", "mass":8.9, "diameter":20.5, "era":"AD", "year":79,
            "inscriptions":"AVG,CAES", "txt":"test_file.txt"}
    test_id = "this-is-a-test-id-0001"
    response = test_client.post(f"/v1/coins/id/{test_id}", json=coin)
    assert response.status_code == 201
    assert response.json()["message"] == "Coin added successfully"
    # Verify coin added to test database
    response = test_client.get(f"/v1/coins/id/{test_id}")
    assert response.status_code == 200
    assert response.json()["name"] == "Test name 1"
    created_at = datetime.strptime(response.json()["created"], r"%Y-%m-%dT%H:%M:%S.%f")
    created_truncated = created_at.replace(second=0, microsecond=0)
    current_datetime_truncated = datetime.now().replace(second=0, microsecond=0)
    assert created_truncated == current_datetime_truncated
    modified_at = datetime.strptime(response.json()["modified"], r"%Y-%m-%dT%H:%M:%S.%f")
    modified_truncated = modified_at.replace(second=0, microsecond=0)
    current_datetime_truncated = datetime.now().replace(second=0, microsecond=0)
    assert modified_truncated == current_datetime_truncated

    # Normal case, missing fields
    coin = {"name":"Test name 2", "catalog":"Test Catalog", "metal":"Gold"}
    test_id = "this-is-a-test-id-0002"
    response = test_client.post(f"/v1/coins/id/{test_id}", json=coin)
    assert response.status_code == 201
    assert response.json()["message"] == "Coin added successfully"

    # Verify coin added to test database
    response = test_client.get(f"/v1/coins/id/{test_id}")
    assert response.status_code == 200
    assert response.json()["name"] == "Test name 2"
    created_at = datetime.strptime(response.json()["created"], r"%Y-%m-%dT%H:%M:%S.%f")
    created_truncated = created_at.replace(second=0, microsecond=0)
    current_datetime_truncated = datetime.now().replace(second=0, microsecond=0)
    assert created_truncated == current_datetime_truncated
    modified_at = datetime.strptime(response.json()["modified"], r"%Y-%m-%dT%H:%M:%S.%f")
    modified_truncated = modified_at.replace(second=0, microsecond=0)
    current_datetime_truncated = datetime.now().replace(second=0, microsecond=0)
    assert modified_truncated == current_datetime_truncated

    # Case with invalid data
    coin = {"name":"Test name 2", "catalog":5, "metal":"Aluminum", "mass":"heavy"}
    test_id = "test-id-0003"
    response = test_client.post(f"/v1/coins/id/{test_id}", json=coin)
    assert response.status_code == 422

    # Case with missing ID
    coin = {"name":"Test name 2", "catalog":"Test Catalog", "metal":"Gold"}
    response = test_client.post("/v1/coins/id/", json=coin)
    assert response.status_code == 404

    # Case with duplicate ID
    coin = {"name":"Test name 2", "catalog":"Test Catalog", "metal":"Gold"}
    test_id = "this-is-a-test-id-0002"
    response = test_client.post(f"/v1/coins/id/{test_id}", json=coin)
    assert response.status_code == 400

# Full coin update endpoint
def test_update_coin(test_client, test_database):

    # Normal case, full coin update
    coin = {"name":"Test name 3", "name_detail":"Test name Detail", 
            "catalog":"Test Catalog", 
            "description":"This is a new test description, still 50 characters min.",
            "metal":"Silver", "mass":4.2, "diameter":15.5, "era":"BC", "year":-40,
            "inscriptions":"AVG,CAES", "txt":"test_file.txt"}
    test_id = "this-is-a-test-id-0002"
    response = test_client.put(f"/v1/coins/id/{test_id}", json=coin)
    assert response.status_code == 200
    assert response.json()["message"] == "Coin updated successfully"
    # Verify coin updated in test database
    response = test_client.get(f"/v1/coins/id/{test_id}")
    assert response.status_code == 200
    assert response.json()["name"] == "Test name 3"
    assert response.json()["mass"] == 4.2
    assert response.json()["era"] == "BC"
    modified_at = datetime.strptime(response.json()["modified"], r"%Y-%m-%dT%H:%M:%S.%f")
    modified_truncated = modified_at.replace(second=0, microsecond=0)
    current_datetime_truncated = datetime.now().replace(second=0, microsecond=0)
    assert modified_truncated == current_datetime_truncated

    # Normal case, partial coin update
    coin = {"name":"Test name 4", "metal":"Silver", "year":90}
    test_id = "this-is-a-test-id-0001"
    response = test_client.put(f"/v1/coins/id/{test_id}", json=coin)
    assert response.status_code == 200
    assert response.json()["message"] == "Coin updated successfully"
    # Verify coin updated in test database
    response = test_client.get(f"/v1/coins/id/{test_id}")
    assert response.status_code == 200
    assert response.json()["name"] == "Test name 4"
    assert "era" not in response.json().keys()
    assert "mass" not in response.json().keys()
    assert "diameter" not in response.json().keys()
    modified_at = datetime.strptime(response.json()["modified"], r"%Y-%m-%dT%H:%M:%S.%f")
    modified_truncated = modified_at.replace(second=0, microsecond=0)
    current_datetime_truncated = datetime.now().replace(second=0, microsecond=0)
    assert modified_truncated == current_datetime_truncated

    # Case with invalid data
    coin = {"name":"Test name 4", "catalog":5, "metal":"Aluminum", "mass":"heavy"}
    test_id = "this-is-a-test-id-0002"
    response = test_client.put(f"/v1/coins/id/{test_id}", json=coin)
    assert response.status_code == 422

    # Case with missing ID
    coin = {"name":"Test name 2", "catalog":"Test Catalog", "metal":"Gold"}
    response = test_client.put("/v1/coins/id/", json=coin)
    assert response.status_code == 404

# Partial coin update endpoint
def test_patch_coin(test_client, test_database):

    # Normal case, full coin update
    coin = {"name":"Test name 5", "name_detail":"Different name Detail", 
            "catalog":"Catalog 17", 
            "description":"This is a another new test description, 50 char min.",
            "metal":"Copper", "mass":1.5, "diameter":10.0, "era":"AD", "year":117,
            "inscriptions":"PON,COS", "txt":"test_file2.txt"}
    test_id = "this-is-a-test-id-0001"
    response = test_client.patch(f"/v1/coins/id/{test_id}", json=coin)
    assert response.status_code == 200
    assert response.json()["message"] == "Coin updated successfully"
    # Verify coin updated in test database
    response = test_client.get(f"/v1/coins/id/{test_id}")
    assert response.status_code == 200
    assert response.json()["name"] == "Test name 5"
    assert response.json()["mass"] == 1.5
    assert response.json()["era"] == "AD"
    assert len(response.json()) == 14
    modified_at = datetime.strptime(response.json()["modified"], r"%Y-%m-%dT%H:%M:%S.%f")
    modified_truncated = modified_at.replace(second=0, microsecond=0)
    current_datetime_truncated = datetime.now().replace(second=0, microsecond=0)
    assert modified_truncated == current_datetime_truncated

    # Normal case, partial coin update
    coin = {"name":"Test name 6", "mass":2.1, "diameter":9.1, "txt":"new_txt.txt"}
    test_id = "this-is-a-test-id-0002"
    response = test_client.patch(f"/v1/coins/id/{test_id}", json=coin)
    assert response.status_code == 200
    assert response.json()["message"] == "Coin updated successfully"
    # Verify coin updated in test database
    response = test_client.get(f"/v1/coins/id/{test_id}")
    assert response.status_code == 200
    assert response.json()["name"] == "Test name 6"
    assert response.json()["mass"] == 2.1
    assert response.json()["era"] == "BC"
    assert response.json()["year"] == -40
    assert response.json()["txt"] == "new_txt.txt"
    assert len(response.json()) == 14
    modified_at = datetime.strptime(response.json()["modified"], r"%Y-%m-%dT%H:%M:%S.%f")
    modified_truncated = modified_at.replace(second=0, microsecond=0)
    current_datetime_truncated = datetime.now().replace(second=0, microsecond=0)
    assert modified_truncated == current_datetime_truncated

    # Case with invalid data
    coin = {"name":"Test name 7", "catalog":5, "metal":"Aluminum", "mass":"heavy"}
    test_id = "this-is-a-test-id-0001"
    response = test_client.patch(f"/v1/coins/id/{test_id}", json=coin)
    assert response.status_code == 422

    # Case with missing ID
    coin = {"name":"Test name 2", "catalog":"Test Catalog", "metal":"Gold"}
    response = test_client.patch("/v1/coins/id/", json=coin)
    assert response.status_code == 404