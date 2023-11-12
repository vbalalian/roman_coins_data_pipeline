#!/usr/bin/env python
# coding: utf-8

# Import libraries
import numpy as np
import pandas as pd
from time import sleep
import requests
from bs4 import BeautifulSoup
import re

# Function to pull html from test pages
def scrape(url_roots: list[str], delay: int = 30):
    combined_pages_html = []
    max_length = 0
    for root in url_roots:
        url = root + 'i.html'
        message = f'requesting {url} ({url_roots.index(root) + 1}/{len(url_roots)})'
        max_length = max(max_length, len(message))
        print(f'\r{message.ljust(max_length)}', end=' ', flush=True)
        sleep(delay) # Delay between requests (wildwinds.com requires 30-seconds)
        page_html = requests.get(url)
        combined_pages_html.append(page_html)
    # After the loop, print a final message that clears the last line
    print(f'\rscraping complete: {url_roots.index(root) + 1}/{len(url_roots)}'.ljust(max_length))
    return combined_pages_html

# Create helper functions for parsing data fields
# Function for parsing names of coin subjects
def pull_title(soup):
    raw_title = soup.find('title').text
    sep_index = raw_title.find(',')
    if sep_index == -1:
        sep_index = raw_title.find('-')
    return raw_title[:sep_index].strip() if sep_index != -1 else raw_title.strip()

# Function to pull subtitles
def pull_subtitle(soup):
    possible_locations = [
        lambda s: s.find_all('h3')[0].contents[-1],
        lambda s: s.find('font').contents[0],
        lambda s: s.find_all('p')[1].contents[-1],
        lambda s: s.find_all('br')[0].contents[0],
    ]
    
    for get_subtitle in possible_locations:
        try:
            subtitle = get_subtitle(soup)
            if not subtitle or len(str(subtitle)) < 4:
                continue
            if any(keyword in str(subtitle) for keyword in ['Click', 'Browse']):
                continue
            if '(' in str(subtitle) or '<' in str(subtitle):
                return None
            return str(subtitle).strip()
        except (IndexError, AttributeError):
            continue 
    
    return None

# Function to pull raw coin data
def pull_coins(soup):
    coins = [coin.contents for coin in soup.find_all('tr') if len(coin) >2 and 'bgcolor' in str(coin)]
    return coins

# Function to pull coin descriptions
def coin_description(coin):
    try:
        description_html = str(coin[1])
        match = re.search(r'<td[^>]*>([^<]+)</td>', description_html)
        if match:
            description = match.group(1)  # The captured group from the regex
            return description.strip()
    except IndexError:
        return None

# Function to identify coin metal
def coin_metal(coin):
    metals = {'#B8':'Copper','#b8':'Copper', '#FF':'Gold', '#C0':'Silver', '#B7':'Brass', '#b7':'Brass', 'red':'FAKE'}
    coin = str(coin)
    try:
        bg_color_index = int(coin.find('bgcolor=')) + 9
        bg_color = coin[bg_color_index:bg_color_index + 3]
        metal = metals[bg_color]
    except:
        return None
    return metal

# Function to pull coin era (i.e. 'AD' or 'BC') 
def coin_era(coin):
    match = re.search(r'\b(AD|BC)\b', str(coin))
    return match.group(0) if match else None

# Function to pull a year (not *every* year) in the coin description
# (if there is a range of years i.e. 117-124 AD, function pulls the year closest to era i.e. '117-124 AD' returns '124', while 'AD 117-124' returns '117')
def coin_year(coin):
    era = coin_era(coin)
    if not era:
        return None

    if era == 'AD':
        # Look for the year pattern before 'AD'
        match = re.search(r'(\d{1,4})(?=\s*AD)', str(coin))
    else:
        # Look for the year pattern before 'BC'
        match = re.search(r'(\d{1,4})(?=\s*BC)', str(coin))

    if not match:
        # If no year is found before the era, search after it
        match = re.search(r'(?<=\bAD\s)(\d{1,4})', str(coin)) if era == 'AD' else re.search(r'(?<=\bBC\s)(\d{1,4})', str(coin))

    if match:
        year = int(match.group(0))
        return year if era == 'AD' else -year
    else:
        return None

# Function to pull .txt urls
def coin_txt(coin):
    for item in coin:
        match = re.search(r'href="([^"]+\.txt)"', str(item))
        if match:
            return match.group(1)

# Function to pull coin ids from jpg or txt urls
def coin_id(coin):
    coin = str(coin)
    match = re.search(r'href="_*([^"]+?)\.(jpg|txt)"', coin)
    if match:
        return match.group(1)

# Function to pull coin mass (in grams)
def coin_mass(coin):
    coin = str(coin)
    gram_variations = [r'\bgr\b', r'\bgm\b', r'\bg\b'] 
    
    def extract_mass(pattern, coin_text):
        match = re.search(r'(\d+(?:\.\d+)?)\s*' + pattern, coin_text)
        if match:
            num_str = match.group(1).replace(',', '.')
            try:
                return float(num_str)
            except ValueError:
                return None
        return None

    for grams in gram_variations:
        mass = extract_mass(grams, coin)
        if mass is not None:
            return mass
            
    return None

# Function to pull coin diameter (in mm)
def coin_diameter(coin):
    coin = str(coin)
    pattern = re.compile(r'(\d+(\.\d*)?)\s*mm')

    match = pattern.search(coin)
    
    if match:
        return float(match.group(1))
    
    return None

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
    coin = f" {str(coin)} "
    inscriptions_list = ['AVG', 'IMP', 'CAES', 'GERM', 'COS', 'CONSVL', 'PP', 'PO', 'PF',
                         'SC', 'CENS', 'TPP', 'TR', 'RESTITVT', 'BRIT', 'AVGVSTVS', 'CAESAR',
                         'C', 'TRIB POT', 'PON MAX', 'PM']
    found_inscriptions = [i for i in inscriptions_list if f' {i} ' in coin]
    unique_inscriptions = list(set(found_inscriptions))
    return unique_inscriptions if unique_inscriptions else None

# Function that combines previous helper functions to return coin DataFrame
def coin_df(soup):
    title = pull_title(soup)
    subtitle = pull_subtitle(soup)
    id, description, metal, mass, diameter, era, year, inscriptions, txt = [], [], [], [], [], [], [], [], []
    for coin in pull_coins(soup):
        id.append(coin_id(coin))
        description.append(coin_description(coin))
        metal.append(coin_metal(coin))
        mass.append(coin_mass(coin))
        diameter.append(coin_diameter(coin))
        era.append(coin_era(coin))
        year.append(coin_year(coin))
        inscriptions.append(coin_inscriptions(coin))
        txt.append(coin_txt(coin))
    return pd.DataFrame({'ruler':title, 'ruler_detail':subtitle, 'id':id, 'description':description, 'metal':metal, 'mass':mass, \
                        'diameter':diameter, 'era':era, 'year':year, 'inscriptions':inscriptions, 'txt':txt})

# Function to combine multiple coin Dataframes
def combine_coin_dfs(soups):
    dfs = [coin_df(soup) for soup in soups]
    filtered_dfs = [df for df in dfs if not df.empty and not df.isna().all().all()]
    return pd.concat(filtered_dfs, ignore_index=True) 

# Pull html from all source pages
''' (pulling from over 200 pages, which takes a couple hours with the 30 second 
delay between requests)'''
def main():
    # Scrape link directory
    with requests.get('https://www.wildwinds.com/coins/ric/i.html') as raw:
        soup = BeautifulSoup(raw.content, 'lxml')

    # Parse html data for a clean list of ruler names
    options = soup.find_all('option')
    emperors_raw = [i.contents for i in options if i.attrs['value'] != ''][:-6]
    emperors = []
    for line in emperors_raw:
        for text in line:
            emperors.append(text.strip())

    # Generate list of usable link roots for each Emperor's coin page
    # wildwinds.com/robots.txt requires a 30-second delay between requests
    linkroots = ['https://www.wildwinds.com/coins/ric/' + i.attrs['value'][:-6] for i in options if i.attrs['value'] != ''][:-6]

    all_pages = scrape(linkroots)

    # Parse html of each page using BeautifulSoup
    all_soups = [BeautifulSoup(page.content, 'lxml') for page in all_pages]

    # Combine it all into a single Dataframe
    roman_coins_raw = combine_coin_dfs(all_soups)

    # Remove duplicate coins
    roman_coins = roman_coins_raw.drop_duplicates(subset=['id'], keep='first').copy()

    # Map average year value of each ruler
    ruler_avg_year = {
        ruler:round(roman_coins[roman_coins['ruler'] == ruler]['year'].mean(), 1) 
        for ruler in roman_coins['ruler'].unique().tolist()
        }

    # Fill missing year values with average year value of matching ruler
    roman_coins.loc[roman_coins['year'].isna(), 'year'] = roman_coins['ruler'].map(ruler_avg_year)

    # Drop remaining NA year values
    roman_coins.dropna(subset='year', inplace=True)
    roman_coins['year'] = roman_coins['year'].astype(int)

    # Map years to eras
    BC_coins = roman_coins['year'] < 0
    AD_coins = roman_coins['year'] > 0

    # Fix era values to match year sign
    roman_coins.loc[BC_coins, 'era'] = 'BC'
    roman_coins.loc[AD_coins, 'era'] = 'AD'

    # Drop entries with missing metal values
    roman_coins.dropna(subset=['metal'], inplace=True)

    # Remove coins with outlier year values
    outlier_coins = (roman_coins['year'] < -50) | (roman_coins['year'] > 500)
    roman_coins = roman_coins[~outlier_coins]

    # Remove entries still missing id value
    roman_coins.dropna(subset=['id'], inplace=True)

    # Consolidate missing value types
    roman_coins.replace([np.inf, -np.inf, np.nan], None, inplace=True)

    # Export roman_coins DataFrame as csv
    roman_coins.to_csv('roman_coins.csv', index=False)

if __name__ == '__main__':
    main()