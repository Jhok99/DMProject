import pymongo
import pandas as pd
import re
from datetime import datetime
import numpy as np
from django.contrib.admin import display

import ast
from bs4 import BeautifulSoup

def clean_from_tags(cleaned_string):

    cleaned_string = re.sub(r'[\n\t\r]', '', cleaned_string)
    cleaned_string = re.sub(r'\\n', '', cleaned_string)
    cleaned_string = re.sub(r'\\t', '', cleaned_string)
    cleaned_string = re.sub(r'\\r', '', cleaned_string)
    cleaned_string = re.sub(r'\'', ' ', cleaned_string)
    cleaned_string = re.sub(r'{', ' ', cleaned_string)
    cleaned_string = re.sub(r'}', ' ', cleaned_string)
    cleaned_string = re.sub(r' minimum: ', ' ', cleaned_string, count=cleaned_string.count("minimum:") - 1).strip()
    cleaned_string = re.sub(r'\s+', ' ', cleaned_string)
    return cleaned_string


def clean_game_document(game):
    field_defaults = {
        "name": "Unknown Game",
        "release_date": "Unknown Date",
        "developer": "Unknown Developer",
        "platforms": [],
        "categories": [],
        "genres": [],
        "tags": [],
        "positive_ratings": 0,
        "negative_ratings": 0,
        "price": 0.0,
        "detailed_description": "No description available",
    }

    for key, default in field_defaults.items():
        if key in game:
            value = game[key]
            if isinstance(value, (list, np.ndarray, pd.Series)):
                if len(value) == 0:
                    game[key] = default
            elif pd.isna(value):
                game[key] = default
        else:
            game[key] = default
    return game


def create_steam_db():
    dataset = ['steam.csv', 'steam_description_data.csv', 'steam_media_data.csv', 'steam_requirements_data.csv',
               'steam_support_info.csv', 'steamspy_tag_data.csv']

    sources = {}
    for source in dataset:
        df = pd.read_csv(source)
        pd.options.display.max_columns = len(df.columns)
        display(df.head(3))
        print(source)
        sources[source] = df

    conn = pymongo.MongoClient("mongodb://localhost:27017/")
    database = conn['steam']

    games_validator = \
        {
            "$jsonSchema": {
                "bsonType": "object",
                "required": [
                    "name",
                    "release_date",
                    "developer",
                    "platforms",
                    "categories",
                    "genres",
                    "tags",
                    "positive_ratings",
                    "negative_ratings",
                    "price",
                    "detailed_description",
                    "linux_requirements",
                    "windows_requirements",
                    "mac_requirements"
                ],
                "properties": {
                    "name": {
                        "bsonType": "string",
                        "description": "The name of the game."
                    },
                    "release_date": {
                        "bsonType": "string",
                        "description": "The release date of the game in string format."
                    },
                    "developer": {
                        "bsonType": "string",
                        "description": "The developer of the game."
                    },
                    "platforms": {
                        "bsonType": "array",
                        "items": {
                            "bsonType": "string"
                        },
                        "description": "The platforms the game is available on (e.g., Windows, Mac, Linux)."
                    },
                    "categories": {
                        "bsonType": "array",
                        "items": {
                            "bsonType": "string"
                        },
                        "description": "The categories of the game."
                    },
                    "genres": {
                        "bsonType": "array",
                        "items": {
                            "bsonType": "string"
                        },
                        "description": "The genres of the game."
                    },
                    "tags": {
                        "bsonType": "array",
                        "items": {
                            "bsonType": "string"
                        },
                        "description": "Tags associated with the game."
                    },
                    "positive_ratings": {
                        "bsonType": "int",
                        "minimum": 0,
                        "description": "The number of positive ratings for the game."
                    },
                    "negative_ratings": {
                        "bsonType": "int",
                        "minimum": 0,
                        "description": "The number of negative ratings for the game."
                    },
                    "price": {
                        "bsonType": "double",
                        "minimum": 0,
                        "description": "The price of the game."
                    },
                    "detailed_description": {
                        "bsonType": "string",
                        "description": "Detailed description of the game."
                    },
                    "linux_requirements": {
                        "bsonType": "string",
                        "description": "Linux system requirements for the game."
                    },
                    "windows_requirements": {
                        "bsonType": "string",
                        "description": "Windows system requirements for the game."
                    },
                    "mac_requirements": {
                        "bsonType": "string",
                        "description": "Mac system requirements for the game."
                    }
                }
            }
        }

    col_games = database.create_collection("games", validator=games_validator)

    s = sources['steam_description_data.csv']

    detailed_descriptions = {}


    for index, record in s.iterrows():
        if record['steam_appid'] not in detailed_descriptions.keys():
            des = str(record['detailed_description'])
            soup = BeautifulSoup(des, 'html.parser')
            cleaned_string = soup.get_text()
            detailed_descriptions[record['steam_appid']]=cleaned_string


    s = sources['steam_requirements_data.csv']

    pc_requirements = {}
    mac_requirements = {}
    linux_requirements = {}
    minimum = {}
    for index, record in s.iterrows():
        if record['steam_appid'] not in pc_requirements.keys():
            pc= str(record['pc_requirements']).lower()
            soup = BeautifulSoup(pc, 'html.parser')
            cleaned_string = soup.get_text()
            cleaned_string = clean_from_tags(cleaned_string)
            pc_requirements[record['steam_appid']]=cleaned_string

        if record['steam_appid'] not in mac_requirements.keys():
            mac= str(record['mac_requirements']).lower()
            soup = BeautifulSoup(mac, 'html.parser')
            cleaned_string = soup.get_text()
            cleaned_string = clean_from_tags(cleaned_string)
            mac_requirements[record['steam_appid']]=cleaned_string

        if record['steam_appid'] not in linux_requirements.keys():
            linux=str(record['linux_requirements']).lower()
            soup = BeautifulSoup(linux, 'html.parser')
            cleaned_string = soup.get_text()
            cleaned_string = clean_from_tags(cleaned_string)
            linux_requirements[record['steam_appid']]=cleaned_string

        if record['steam_appid'] not in minimum.keys():
            mini=str(record['minimum']).lower()
            soup = BeautifulSoup(mini, 'html.parser')
            cleaned_string = soup.get_text()
            cleaned_string = clean_from_tags(cleaned_string)
            minimum[record['steam_appid']]=cleaned_string

    s = sources['steam.csv']

    game = {}

    for index, record in s.iterrows():
        game['_id']=record['appid']
        game['name'] = record['name']
        game['release_date'] = record['release_date']
        game['developer'] = record['developer']
        game['platforms'] = []
        game['categories'] = []
        game['genres'] = []
        game['tags'] = []
        game['positive_ratings'] = record['positive_ratings']
        game['negative_ratings'] = record['negative_ratings']
        game['price'] = record['price']
        for i in record['platforms'].split(';'):
            game['platforms'].append(i)
        for c in record['categories'].split(';'):
            game['categories'].append(c)
        for g in record['genres'].split(';'):
            game['genres'].append(g)
        for t in record['steamspy_tags'].split(';'):
            game['tags'].append(t)
        game['detailed_description'] = str(detailed_descriptions[record['appid']])
        if record['appid'] in minimum.keys():
            game['minimum'] = str(minimum[record['appid']])
        if record['appid'] in pc_requirements.keys() or record['appid'] in mac_requirements.keys() or record[
            'appid'] in linux_requirements.keys():
            if 'linux' in game['platforms'] and str(linux_requirements[record['appid']])!="[]":
                game['linux_requirements'] = str(linux_requirements[record['appid']])
            else:
                game['linux_requirements'] = "No Data Available"
            if 'windows' in game['platforms'] and str(pc_requirements[record['appid']])!="[]":
                game['windows_requirements'] = str(pc_requirements[record['appid']])
            else:
                game['windows_requirements'] = "No Data Available"
            if 'mac' in game['platforms'] and str(mac_requirements[record['appid']])!="[]":
                game['mac_requirements'] = str(mac_requirements[record['appid']])
            else:
                game['mac_requirements'] = "No Data Available"


        game = clean_game_document(game)
        col_games.insert_one(game)
        game = {}




def main():
    try:
        create_steam_db()
    except Exception as e:
        print(f"Database already exists or caught error:{e}")

    conn = pymongo.MongoClient("mongodb://localhost:27017/")
    database = conn['steam']
    col_games=database["games"]
    query={
        #"positive_ratings": {"gte":124532, "$lte":124536},
        "price":{"$gte":5.0,"$lte":10.0},
    }
    projection={
        "_id":0,
        "name":1,
        "windows_requirements":1
    }
    result=list(col_games.find(query,projection))
    for r in result:
        print(r,'\n')

if __name__ == '__main__':
    main()