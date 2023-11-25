#!/usr/bin/env python
# coding: utf-8

# Import libraries
import numpy as np
import pandas as pd
from time import sleep
import requests
from bs4 import BeautifulSoup
import re
import os
import psycopg2
from psycopg2.extras import RealDictCursor

# Check if web_scraper has already run
flag_file_path = '/app/data/scraping_done.txt'
if os.path.exists(flag_file_path):
    print('Scraping already completed. Exiting.')
    exit()

# Path to roman_coins database
db_name = os.getenv('DB_NAME', 'roman_coins')
db_user = os.getenv('DB_USER', 'postgres')
db_password = os.getenv('DB_PASSWORD', 'postgres')
db_host = 'db'

# Database connection manager
def connect_db():
    '''Returns a connection with PostgreSQL db at path'''
    try:
        conn = psycopg2.connect(
            dbname=db_name, 
            user=db_user, 
            password=db_password, 
            host=db_host,
            cursor_factory=RealDictCursor)
    except psycopg2.Error as e:
        print("Connection error:", e)
    if conn:
        return conn
    
table_name = 'roman_coins'
table_columns = ['ruler', 'ruler_detail', 'id', 'description', 'metal', 
                 'mass', 'diameter', 'era', 'year', 'inscriptions', 'txt']
column_dtypes = ['VARCHAR(30)', 'VARCHAR(1000)', 'VARCHAR(80)', 'VARCHAR(1000)', 'VARCHAR(20)', 'REAL', 
                 'REAL', 'VARCHAR(10)', 'REAL', 'VARCHAR(100)', 'VARCHAR(100)']

def create_table(conn:psycopg2.extensions.connection, table:str, cols:list, dtypes:list):
    '''Creates a table based on input connection & parameters'''
    try:
        cur = conn.cursor()
        cur.execute(f'CREATE TABLE IF NOT EXISTS {table} (' + 
                    ', '.join(f'{col} {dtype}' for col, dtype 
                                in zip(cols, dtypes)) + ');')
    except Exception as e:
        print("Create Table error:", e)
    finally:
        cur.close()

# Function to pull link roots for each "emperor" page
def get_linkroots(page:str):
    # Scrape link directory
    with requests.get(page + 'i.html') as raw:
        soup = BeautifulSoup(raw.content, 'lxml')

    # Parse html data for a clean list of ruler names
    options = soup.find_all('option')
    emperors_raw = [i.contents for i in options if i.attrs['value'] != ''][:-6]
    emperors = []
    for line in emperors_raw:
        for text in line:
            emperors.append(text.strip())

    # Generate list of usable link roots for each Emperor's coin page
    linkroots = [page + i.attrs['value'][:-6] for i in options if i.attrs['value'] != ''][:-6]
    return linkroots



# Create helper functions for parsing data fields
# Function for parsing names of coin subjects
def pull_title(soup):
    raw_title = soup.find('title')
    if raw_title:
        title_text = raw_title.text
        sep_index = title_text.find(',')
        if sep_index == -1:
            sep_index = title_text.find('-')
        return title_text[:sep_index].strip() if sep_index != -1 else title_text.strip()
    return None

def pull_subtitle(soup):
    # Filter function to exclude unwanted text
    def valid_subtitle(text):
        if not text or len(text) < 4:
            return False
        unwanted_phrases = ['Click', 'Browse', 'Main Page', 'RPC', 'RIC', 'Sear', 
                            'NOTE:', 'Note:', 'NOW', 'NEW', 'example', 'examples', 
                            'RSC']
        if any(phrase in text for phrase in unwanted_phrases):
            return False
        return True

    # Check directly after <h2> tags
    h2_subtitle = soup.find('h2')
    if h2_subtitle and h2_subtitle.next_sibling and h2_subtitle.next_sibling.string:
        subtitle = h2_subtitle.next_sibling.string.strip()
        if valid_subtitle(subtitle):
            return subtitle

    # Check within <p> tags for subtitles
    for p_tag in soup.find_all('p'):
        p_text = p_tag.get_text(strip=True)
        if valid_subtitle(p_text):
            return p_text

    # Fallback: Check within <font> tags
    font_tag = soup.find('font')
    if font_tag:
        font_text = font_tag.get_text(strip=True)
        if valid_subtitle(font_text):
            return font_text

    return None

# Function to pull raw coin data
def pull_coins(soup):
    coins = [coin.contents for coin in soup.find_all('tr') if len(coin) >2 and 'bgcolor' in str(coin)]
    return coins

# Function to pull coin ids
def coin_id(coin):
    try:
        id_html = coin[0]
        id = id_html.get_text()
        return id if id else None
    except IndexError:
        return None

# Function to pull coin descriptions
def coin_description(coin):
    try:
        for i in range(1, 4):
            chunk = coin[i]
            desc = chunk.get_text().replace('\r\n', '')
            if len(desc) >= 50:
                return desc
    except IndexError:
        return None

# Function to identify coin metal
def coin_metal(coin):
    metals = {
        '#B87334':'Copper', '#B87333':'Copper', '#b87333':'Copper', 
        '#FFD700':'Gold', '#ffd700':'Gold', '#FFD660':'Gold', '#FFC601':'Gold', 
        '#C0C0C0':'Silver', '#c0C0C0':'Silver', '#c0c0c0':'Silver', '#cc9966':'Silver',
        '#B7A642':'Bronze', '#b7a642':'Bronze', 
        '#AC9B88':'Lead', 
        '#FFF7A3':'Bone',
        'red':'FAKE', '#F34629':'FAKE', '#FA6262':'FAKE', '#FF0000':'FAKE', '#FF4040':'FAKE', '#FA6036':'FAKE'}
    try:
        color = list(coin[0].attrs.values())[0]
        return metals[color]
    except:
        return None

# Function to pull coin era (i.e. 'AD' or 'BC') 
def coin_era(coin):
    description = coin_description(coin)
    try:
        match = re.search(r'\b(AD|BC)\b', description)
        return match.group(0)
    except:
        return None

# Function to pull first year in the coin description
def coin_year(coin):
    description = coin_description(coin)
    try:
        year_matches = re.findall(r'\b(AD|BC)\s*(\d{1,3})(?:/(\d{1,3})|-(\d' +
                                  r'{1,3}))?\b|\b(\d{1,3})(?:/(\d{1,3})|-(' +
                                  r'\d{1,3}))?\s*(AD|BC)\b', description)
    except:
        return None

    valid_years = []
    for match in year_matches:
        era1, start_year1, end_year1_slash, end_year1_dash, start_year2, end_year2_slash, end_year2_dash, era2 = match
        era = era1 or era2
        start_year = int(start_year1 or start_year2)
        end_year = int(end_year1_slash or end_year1_dash or end_year2_slash or 
                       end_year2_dash) if end_year1_slash or end_year1_dash or \
                        end_year2_slash or end_year2_dash else None

        year = start_year

        if year:
            valid_years.append(year if era not in ['BC', 'B.C.'] else -year)

    return min(valid_years) if valid_years else None

# Function to pull .txt urls
def coin_txt(coin, title):
    base_url = 'https://www.wildwinds.com/coins/ric/'
    for level in [2, 1, 3, 4, 5]:
        try:
            for a in coin[level].find_all('a', href=True):
                filename = a['href']
                if '.txt' in filename:
                    return base_url + title.replace(' ', '_').lower() + '/' + filename
        except:
            continue
    return None

# Function to pull coin mass (in grams)
def coin_mass(coin):
    try:
        description = coin_description(coin)
        match = re.search(r'(\d+((\.|\,|\-)\d+)?)\s?(?:g|gm|gr)\b', description)
        return float(match.group(1).replace(',', '.').replace('-', '.'))
    except:
        return 0.0

# Function to pull coin diameter (in mm)
def coin_diameter(coin):
    try:
        description = coin_description(coin)
        match = re.search(r'(\d{1,2}(\.\d+)?)\s?(?:mm)', description)
        diameter = float(match.group(1))
        return diameter if diameter else 0
    except:
        return 0.0

# Check for common inscriptions
''' ...such as "AVG" (Augustus, title of the emperor), "IMP" (Imperator 
(victorious general), received upon accession), "CAES" (Caesar, inherited name 
of the Julian family (Julius Caesar), used by later emperors to designate heir), 
"GERM" (Germanicus, a title honoring military victories in Germany), "COS" or 
"CONSVL" (Consul, a title linked to highest office in Senate, usually held by 
emperor), "PO" (Pontifex Maximus, highest priest, the head of state religion), 
"PP" (Pater Patriae, father of the country), "PF" (Pius Felix, reverent or 
dutiful), "SC" (Senatus Consultus), "TPP" (Tribunica Potestate, tribune of the 
people, each renewal indicated by numerals), "CENS" (Censor, a public office 
overseeing taxes, morality, the census and membership in various orders), 
"BRIT" (Britannicus).'''

# Function to pull recognized inscriptions
def coin_inscriptions(coin):
    inscriptions_list = ['AVG', 'IMP', 'CAES', 'GERM', 'COS', 'CONSVL', 'PP', 
                         'PO', 'PF', 'SC', 'CENS', 'TPP', 'TR', 'RESTITVT', 
                         'BRIT', 'AVGVSTVS', 'CAESAR', 'C', 'TRIB', 'POT', 'PON',
                         'MAX', 'PM', 'SPQR', 'S P Q R', 'S-C', 'TRP', 'PAX']
    try:
        description = coin_description(coin)
        inscriptions = {i for i in inscriptions_list if f' {i} ' in description or f' {i},' in description}
        return ','.join(sorted(list(inscriptions))) if inscriptions else None
    except:
        return None

# Function that combines previous helper functions to return coins dicts
def coins_from_soup(soup:BeautifulSoup):
    title = pull_title(soup)
    subtitle = pull_subtitle(soup)
    coins = []
    for coin in pull_coins(soup):
        coins.append({
            'ruler':title,
            'ruler_detail':subtitle,
            'id':coin_id(coin),
            'description':coin_description(coin),
            'metal':coin_metal(coin),
            'mass':coin_mass(coin),
            'diameter':coin_diameter(coin),
            'era':coin_era(coin),
            'year':coin_year(coin),
            'inscriptions':coin_inscriptions(coin),
            'txt':coin_txt(coin, title=title)
        })

    return coins if coins else None

# Function to pull html from test pages
def scrape_page(url_root: str):
    url = url_root + 'i.html'
    html = requests.get(url)
    soup = BeautifulSoup(html.content, 'lxml')
    return soup

def load_coins(coins:list[dict] | None, conn:psycopg2.extensions.connection, table:str):
    try:
        cur = conn.cursor()
        for coin in coins:
            columns = ', '.join(coin.keys())
            placeholders = ', '.join(f'%({col})s' for col in coin.keys())
            query = f'INSERT INTO {table} ({columns}) VALUES ({placeholders});'
            cur.execute(query, coin)
        conn.commit()            
    except psycopg2.Error as e:
        print('Load error:', e)
        conn.rollback()
    finally:
        cur.close()

def scrape_and_load(conn:psycopg2.extensions.connection, linkroots:list[str], table:str, delay:int=30):
    for root in linkroots:
        print(f'requesting {root + "i.html"} ({linkroots.index(root) + 1}/{len(linkroots)})')
        sleep(delay)
        soup = scrape_page(root)
        coins = coins_from_soup(soup)
        if coins:
            print(f'loading coins into database: {db_name} at {db_host} as {db_user}')
            load_coins(coins, conn, table)
    print(f'Scraping/Loading complete: {len(linkroots)} pages')

# Pull html from all source pages
def main():
    ''' (pulling from over 200 pages, which takes a couple hours with the 30 
    second delay between requests)'''
    linkroots = get_linkroots('https://www.wildwinds.com/coins/ric/')
    with connect_db() as conn:
        create_table(conn, table_name, table_columns, column_dtypes)
        scrape_and_load(conn, linkroots, table_name)
    conn.close()

    # Flag web_scraping completion
    with open(flag_file_path, 'w') as file:
        file.write('done')

if __name__ == '__main__':
    main()