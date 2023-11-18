import sqlite3
import csv

db_path = 'api/roman_coins.db'
csv_path = 'web_scraping/roman_coins.csv'
table_name = 'roman_coins'
table_columns = ['ruler', 'ruler_detail', 'id', 'description', 'metal', 
                 'mass', 'diameter', 'era', 'year', 'inscriptions', 'txt']
column_dtypes = ['TEXT', 'TEXT', 'TEXT PRIMARY KEY', 'TEXT', 'TEXT', 'REAL', 
                 'REAL', 'TEXT', 'INTEGER', 'TEXT', 'TEXT']

def connect_db(path:str):
    '''Returns a connection with SQLite db at path'''
    try:
        con = sqlite3.connect(path)
        con.row_factory = sqlite3.Row
        return con
    except sqlite3.Error as e:
        print("SQLite error (connect_db):", e)

def csv_rows(path:str):
    '''Reads CSV file at path, returns a list containing each row as a dictionary.'''
    with open(path, 'r') as file:
        dr = csv.DictReader(file)
        return list(dr)

def create_table(con:sqlite3.Connection, table:str, cols:list, dtypes:list):
    '''Creates a table based on input connection & parameters'''
    try:
        cur = con.cursor()
        cur.execute(f'CREATE TABLE IF NOT EXISTS {table} (' + 
                    ', '.join(f'{col} {dtype}' for col, dtype 
                                in zip(cols, dtypes)) + ');')
    except sqlite3.Error as e:
        print("SQLite error (create_table):", e)

def load_table(con:sqlite3.Connection, table:str, rows:list[dict]):
    '''Loads table columns with values'''
    try:
        cur = con.cursor()
        for row in rows:
            columns = ', '.join(row.keys())
            placeholders = ', '.join(f':{col}' for col in row.keys())
            query = f'INSERT INTO {table} ({columns}) VALUES ({placeholders});'
            cur.execute(query, row)
        con.commit()
    except sqlite3.Error as e:
        print("SQLite error (load_table):", e)

def main(con:sqlite3.Connection, table:str, columns:list, dtypes:list, rows:list[dict]):
    create_table(con, table, columns, dtypes)
    load_table(con, table, rows)

if __name__ == '__main__':
    roman_coin_rows = csv_rows(csv_path)
    con = connect_db(db_path)
    main(con, table_name, table_columns, column_dtypes, roman_coin_rows)