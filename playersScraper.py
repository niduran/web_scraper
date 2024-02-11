import csv
import os
import re
import sys
import uuid
from datetime import datetime

import pandas as pd
import psycopg2
import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from sqlalchemy import create_engine

def connect_postgres():
    load_dotenv()
    conn = psycopg2.connect(
        database=os.getenv('DATABASE'),
        user=os.getenv('USER'),
        password=os.getenv('PASSWORD'),
        host=os.getenv('HOST')
    )
    conn.autocommit = True
    return conn

def create_table(conn):
    cur = conn.cursor()

    create_table_query = """
    CREATE TABLE IF NOT EXISTS players (
        playerid TEXT PRIMARY KEY,
        url TEXT UNIQUE,
        name TEXT,
        full_name TEXT,
        date_of_birth DATE,
        age INTEGER,
        place_of_birth TEXT,
        country_of_birth TEXT,
        position TEXT,
        current_club TEXT,
        national_team TEXT,
        appearances_current_club INTEGER,
        goals_current_club INTEGER,
        scraping_timestamp TIMESTAMP
    );
    """
    cur.execute(create_table_query)

def import_data():
    df = pd.read_csv('playersData.csv', sep=';')
    engine = create_engine(f"postgresql://{os.getenv('USER')}:{os.getenv('PASSWORD')}@{os.getenv('HOST')}/{os.getenv('DATABASE')}")

    existing_columns = pd.read_sql_query("SELECT column_name FROM information_schema.columns WHERE table_name = 'players'", engine)
    existing_columns = existing_columns['column_name'].tolist()
    df.columns = df.columns.str.lower().str.replace(' ', '_')

    columns_to_insert = [col for col in df.columns if col.lower() in existing_columns]
    df_subset = df[columns_to_insert]
    df_subset = df_subset.drop_duplicates(subset=["url"], keep='last')
    df_subset.loc[:, "date_of_birth"] = pd.to_datetime(df_subset["date_of_birth"], format='%d.%m.%Y')
    try:
        df_subset.to_sql("players", engine, if_exists='append', index=False)
        print("Data imported successfully from csv.")
    except Exception as e:
        print("Error:", e)

def load_scraped_data(conn, file_name_urls):
    cur = conn.cursor()
    with open(file_name_urls, 'r') as csvfile:
        csvreader = csv.reader(csvfile)
        for url in csvreader:
            player_info = web_scraper(url[0])
            if player_info:
                insert_query = """
                INSERT INTO players(
                    playerid, 
                    name, 
                    full_name, 
                    url, 
                    date_of_birth, 
                    age, 
                    place_of_birth, 
                    country_of_birth, 
                    position, 
                    current_club, 
                    national_team, 
                    appearances_current_club, 
                    goals_current_club, 
                    scraping_timestamp
                )
                VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP AT TIME ZONE 'UTC'
                )
                ON CONFLICT (url) DO UPDATE
                SET 
                    name = EXCLUDED.name, 
                    full_name = EXCLUDED.full_name, 
                    date_of_birth = EXCLUDED.date_of_birth, 
                    age = EXCLUDED.age, 
                    place_of_birth = EXCLUDED.place_of_birth, 
                    country_of_birth = EXCLUDED.country_of_birth, 
                    position = EXCLUDED.position, 
                    current_club = EXCLUDED.current_club, 
                    national_team = EXCLUDED.national_team, 
                    appearances_current_club = EXCLUDED.appearances_current_club, 
                    goals_current_club = EXCLUDED.goals_current_club, 
                    scraping_timestamp = EXCLUDED.scraping_timestamp;
                """

                cur.execute(insert_query, (
                    str(uuid.uuid4()), player_info['Name'], player_info['Full name'], url[0], player_info['Date of birth'],
                    player_info['Age'], player_info['Place of birth'], player_info['Country of birth'], player_info['Position(s)'],
                    player_info['Current team'], player_info['National_team'], player_info['Appearances current club'], player_info['Goals_current_club']
                ))

def web_scraper(url):
    r = requests.get(url)
    soup = BeautifulSoup(r.content, 'html.parser')
    player_info = {
        'Name': None,
        'Full name': None,
        'Date of birth': None,
        'Age': None,
        'Place of birth': None,
        'Country of birth': None,
        'Position(s)': None,
        'Current team': None,
        'National_team': None,
        'Appearances current club': None,
        'Goals_current_club': None,
    }

    key_list_wiki = ['Name', 'Full name', 'Date of birth', 'Place of birth', 'Position(s)', 'Current team', 'National_team']
    body_content = soup.find('div', {'id': 'bodyContent'})
    is_footballer = False
    if body_content:
        infoboxes = body_content.find_all('table', {'class': 'infobox vcard'})
        for infobox in infoboxes:
            player_info['Name'] = infobox.find('caption').text.strip()
            rows = infobox.find_all('tr')
            for row in rows:
                headers = row.find_all('th')
                if headers:
                    header_text = headers[0].text.strip()
                    if header_text in key_list_wiki:
                        if header_text == 'Date of birth':
                            age_text = row.find('td').text.strip()
                            date_of_birth_match = re.search(r'\((\d{4}-\d{2}-\d{2})\)', age_text)
                            player_info['Date of birth'] = date_of_birth_match.group(1) if date_of_birth_match else None
                            age_match = re.search(r'\(age\s*(\d+)\)', age_text)
                            player_info['Age'] = int(age_match.group(1)) if age_match else None
                        elif header_text == 'Place of birth':
                            place_birth = row.find('td').text.strip()
                            match = re.match(r'^(.*),\s*(.*)$', place_birth)
                            if match:
                                player_info['Place of birth'] = match.group(1).strip()
                                player_info['Country of birth'] = match.group(2).strip()
                            else:
                                player_info['Country of birth'] = place_birth.strip()
                        elif header_text == 'Current team':
                            current_club = row.find('td').text.strip()
                            player_info['Current team'] = current_club.split('(')[0].strip()
                        else:
                            player_info[header_text] = row.find('td').text.strip()
                    elif 'Senior career' in header_text:
                        is_footballer = True
                        apps = 0
                        goals = 0
                        senior_career_row = row.find_next_sibling('tr')
                        while senior_career_row:
                            cells = senior_career_row.find_all('td')
                            if len(cells) == 3:
                                team_name = cells[0].text.strip()
                                team_name = team_name.split('→')[-1].split('(')[0].strip()
                                if team_name == player_info['Current team']:
                                    app = cells[1].text.strip()
                                    if app.isdigit():
                                        apps = apps + int(app)
                                    else:
                                        apps = None
                                    goal = cells[2].text.strip()[1:-1]
                                    if goal.isdigit():
                                        goals = goals + int(goal)
                                    else:
                                        goals = None
                                player_info['Appearances current club'] = apps
                                player_info['Goals_current_club'] = goals
                                senior_career_row = senior_career_row.find_next_sibling('tr')
                            else:
                                senior_career_row = None
                    elif 'International career' in header_text:
                        is_footballer = True
                        international_career_row = row.find_next_sibling('tr')
                        while international_career_row:
                            year_cells = international_career_row.find('th')
                            data_cells = international_career_row.find_all('td')
                            if len(data_cells) == 3:
                                national_team_date = year_cells.text.strip()
                                if re.search(r'–\s*$', national_team_date):
                                    player_info['National_team'] = data_cells[0].text.strip()
                                international_career_row = international_career_row.find_next('tr')
                            else:
                                international_career_row = None

    for key, value in player_info.items():
        if isinstance(value, str) and value.endswith(']'):
            player_info[key] = value.split('[')[0].strip()

    if is_footballer:
        return player_info
    else:
        return None

if __name__ == "__main__":
    conn = connect_postgres()
    create_table(conn)
    import_data()
    file_name_urls =  sys.argv[1]
    load_scraped_data(conn, file_name_urls)
