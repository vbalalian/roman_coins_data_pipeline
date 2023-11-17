import sqlite3
import csv

db_path = 'api/roman_coins.db'
csv_path = 'web_scraping/roman_coins.csv'

def main(db_path = db_path):
    try:
        with sqlite3.connect(db_path) as con:
            cur = con.cursor()
            cur.execute('''CREATE TABLE IF NOT EXISTS roman_coins (
                        ruler TEXT,
                        ruler_detail TEXT,
                        id TEXT PRIMARY KEY,
                        description TEXT,
                        metal TEXT,
                        mass REAL,
                        diameter REAL,
                        era TEXT,
                        year INTEGER,
                        inscriptions TEXT,
                        txt TEXT
            );''')
        
            with open(csv_path, 'r') as file:
                dr = csv.DictReader(file)
                for row in dr:
                    cur.execute('''INSERT INTO roman_coins VALUES (:ruler, :ruler_detail, \
                                :id, :description, :metal, :mass, :diameter, :era, :year, \
                                :inscriptions, :txt);''', row)
        
    except sqlite3.Error as e:
        print("SQLite error:", e)

    finally:
        if con:
            con.close()

if __name__ == '__main__':
    main()