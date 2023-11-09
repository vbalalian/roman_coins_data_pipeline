import sqlite3
import csv

db_path = './sqlite3/roman_coins.db'
csv_path = '../web_scraping/roman_coins.csv'

con = sqlite3.connect(db_path)
cur = con.cursor()

try:
    cur.execute('''CREATE TABLE IF NOT EXISTS roman_coins (
                ruler,
                ruler_detail,
                id,
                description,
                metal,
                mass,
                diameter,
                era,
                year,
                inscriptions,
                txt
    );''')
    
    with open(csv_path, 'r') as file:
        dr = csv.DictReader(file)
    
        for row in dr:
            cur.execute('''INSERT INTO roman_coins VALUES (:ruler, :ruler_detail, \
                        :id, :description, :metal, :mass, :diameter, :era, :year, \
                        :inscriptions, :txt);''', row)
    
    con.commit()

except sqlite3.Error as e:
    print("SQLite error:", e)
    con.rollback()

con.close()