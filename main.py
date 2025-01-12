import pymongo
import pandas as pd
import re
from datetime import datetime
import numpy as np
from django.contrib.admin import display
import ast
from bs4 import BeautifulSoup
from flask import Flask, request, jsonify,Response
import csv



app = Flask(__name__)
conn = pymongo.MongoClient("mongodb://localhost:27017/")
database = conn['steam']
@app.route('/games', methods=['GET'])
def get_games():
    query = request.args.to_dict()
    projection = {"_id": 0, "name": 1, "price": 1, "genres": 1, "positive_ratings": 1}
    results = list(database.games.find(query, projection))
    return jsonify(results)

@app.route('/reports/top_genres', methods=['GET'])
def top_genres():
    pipeline = [
        {"$unwind": "$genres"},
        {"$group": {"_id": "$genres", "average_rating": {"$avg": "$positive_ratings"}}},
        {"$sort": {"average_rating": -1}},
        {"$limit": 5}
    ]
    result = list(database.games.aggregate(pipeline))
    return jsonify(result)


#examplequery...////games/export?min_price=5&max_price=20
@app.route('/games/export', methods=['GET'])
def export_games():
    min_price = float(request.args.get('min_price', 0))
    max_price = float(request.args.get('max_price', 100))
    query = {"price": {"$gte": min_price, "$lte": max_price}}

    # Fetch games from MongoDB
    games = list(database.games.find(query, {"_id": 0, "name": 1, "price": 1, "positive_ratings": 1, "tags": 1}))

    # Debug: Print fetched games
    print("Games fetched:", games)

    # Return a response if no games are found
    if not games:
        return jsonify({"error": "No games found matching the criteria"}), 404

    # CSV generation
    def generate_csv():
        fieldnames = ["name", "price", "positive_ratings", "tags"]
        output = [','.join(fieldnames)]
        for game in games:
            row = [str(game.get(field, "")) for field in fieldnames]
            output.append(','.join(row))
        return '\n'.join(output)

    # Return CSV as a downloadable file
    csv_data = generate_csv()
    return Response(csv_data, mimetype='text/csv', headers={"Content-Disposition": "attachment;filename=games.csv"})

@app.route('/reports/price-trend', methods=['GET'])
def price_trend():
    pipeline = [
        {
            "$addFields": {  # Extract year from release_date
                "release_year": {"$substr": ["$release_date", 0, 4]}
            }
        },
        {"$group": {"_id": "$release_year", "average_price": {"$avg": "$price"}}},
        {"$sort": {"_id": 1}}
    ]
    result = list(database.games.aggregate(pipeline))
    return jsonify(result)

@app.route('/recommendations/<game_name>', methods=['GET'])
def recommend_games(game_name):
    # Find the game by name
    game = database.games.find_one({"name": {"$regex": f"^{game_name}$", "$options": "i"}}, {"_id": 0, "tags": 1, "genres": 1})
    if not game:
        return jsonify({"error": "Game not found"}), 404

    # Find similar games based on tags and genres
    query = {
        "$or": [
            {"tags": {"$in": game["tags"]}},
            {"genres": {"$in": game["genres"]}}
        ],
        "name": {"$ne": game_name}  # Exclude the original game from recommendations
    }
    projection = {"_id": 0, "name": 1, "tags": 1, "genres": 1, "price": 1, "positive_ratings": 1}
    recommendations = list(database.games.find(query, projection).limit(10))

    return jsonify({"recommendations": recommendations})


@app.route('/requirements/<game_name>/<system>', methods=['GET'])
def get_system_requirements(game_name, system):
    # Validate the system parameter
    valid_systems = ['windows', 'mac', 'linux']
    if system.lower() not in valid_systems:
        return jsonify({"error": f"Invalid system '{system}'. Valid options are {valid_systems}"}), 400

    # Query the database for the game
    query = {"name": {"$regex": f"^{game_name}$", "$options": "i"}}
    projection = {"_id": 0, f"{system.lower()}_requirements": 1, "name": 1}
    game = database.games.find_one(query, projection)

    # Handle case where the game is not found
    if not game:
        return jsonify({"error": f"Game '{game_name}' not found"}), 404

    # Handle case where system requirements are not available
    requirements = game.get(f"{system.lower()}_requirements", "No Data Available")
    if requirements == "No Data Available":
        return jsonify({"message": f"System requirements for '{system}' are not available for '{game_name}'"}), 404

    # Return the system requirements
    return jsonify({
        "game": game_name,
        "system": system,
        "requirements": requirements
    })

@app.route('/games/<game_id>', methods=['PUT'])
def edit_game(game_id):
    data = request.get_json()

    # Validate if the game exists
    game = database.games.find_one({"_id": int(game_id)})
    if not game:
        return jsonify({"error": f"Game with ID {game_id} not found"}), 404

    # Update the game
    try:
        database.games.update_one({"_id": int(game_id)}, {"$set": data})
        return jsonify({"message": "Game updated successfully"}), 200
    except Exception as e:
        return jsonify({"error": f"Failed to update game: {str(e)}"}), 500

@app.route('/games/<game_id>', methods=['DELETE'])
def delete_game(game_id):
    # Validate if the game exists
    game = database.games.find_one({"_id": int(game_id)})
    if not game:
        return jsonify({"error": f"Game with ID {game_id} not found"}), 404

    # Delete the game
    try:
        database.games.delete_one({"_id": int(game_id)})
        return jsonify({"message": f"Game with ID {game_id} deleted successfully"}), 200
    except Exception as e:
        return jsonify({"error": f"Failed to delete game: {str(e)}"}), 500

@app.route('/games', methods=['POST'])
def add_game():
    data = request.get_json()

    # Check if the required fields are provided
    required_fields = ["name", "release_date", "developer", "platforms", "categories", "genres", "tags", "positive_ratings", "negative_ratings", "price", "detailed_description"]
    missing_fields = [field for field in required_fields if field not in data]

    if missing_fields:
        return jsonify({"error": f"Missing fields: {missing_fields}"}), 400

    # Insert the game into the database
    try:
        database.games.insert_one(data)
        return jsonify({"message": "Game added successfully"}), 201
    except Exception as e:
        return jsonify({"error": f"Failed to add game: {str(e)}"}), 500


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
    for _, record in s.iterrows():
        appid = record['steam_appid']
        pc_requirements[appid] = clean_from_tags(
            BeautifulSoup(str(record.get('pc_requirements', "No Data Available")).lower(), 'html.parser').get_text()
        )
        mac_requirements[appid] = clean_from_tags(
            BeautifulSoup(str(record.get('mac_requirements', "No Data Available")).lower(), 'html.parser').get_text()
        )
        linux_requirements[appid] = clean_from_tags(
            BeautifulSoup(str(record.get('linux_requirements', "No Data Available")).lower(), 'html.parser').get_text()
        )

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
        game['linux_requirements']= linux_requirements.get(record['appid'], "No Data Available")
        if game['linux_requirements']=="[]":
            game['linux_requirements'] = 'No Data Available'
        game['windows_requirements']= pc_requirements.get(record['appid'], "No Data Available")
        if game['windows_requirements']=="[]":
            game['windows_requirements'] = 'No Data Available'
        game['mac_requirements']= mac_requirements.get(record['appid'], "No Data Available")
        if game['mac_requirements']=="[]":
            game['mac_requirements'] = 'No Data Available'

        game = clean_game_document(game)
        col_games.insert_one(game)
        game = {}




def main():
    try:
        create_steam_db()
    except Exception as e:
        print(f"Database already exists or caught error:{e}")



if __name__ == '__main__':
    main()
    app.run(debug=True)
