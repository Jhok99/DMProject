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
    min_price = float(request.args.get('min_price', 5))
    max_price = float(request.args.get('max_price', 10))
    query = {"price": {"$gte": min_price, "$lte": max_price}}

    games = list(database.games.find(query, {"_id": 0, "name": 1, "price": 1, "positive_ratings": 1, "tags": 1}))

    print("Games fetched:", games)

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
    game = database.games.find_one({"name": {"$regex": f"^{game_name}$", "$options": "i"}}, {"_id": 0, "tags": 1, "genres": 1})
    if not game:
        return jsonify({"error": "Game not found"}), 404

    query = {
        "$or": [
            {"tags": {"$in": game["tags"]}},
            {"genres": {"$in": game["genres"]}}
        ],
        "name": {"$ne": game_name}
    }
    projection = {"_id": 0, "name": 1, "tags": 1, "genres": 1, "price": 1, "positive_ratings": 1}
    recommendations = list(database.games.find(query, projection).limit(10))

    return jsonify({"recommendations": recommendations})


@app.route('/requirements/<game_name>/<system>', methods=['GET'])
def get_system_requirements(game_name, system):
    valid_systems = ['windows', 'mac', 'linux']
    if system.lower() not in valid_systems:
        return jsonify({"error": f"Invalid system '{system}'. Valid options are {valid_systems}"}), 400

    query = {"name": {"$regex": f"^{game_name}$", "$options": "i"}}
    projection = {"_id": 0, f"{system.lower()}_requirements": 1, "name": 1}
    game = database.games.find_one(query, projection)

    if not game:
        return jsonify({"error": f"Game '{game_name}' not found"}), 404

    requirements = game.get(f"{system.lower()}_requirements", "No Data Available")
    if requirements == "No Data Available":
        return jsonify({"message": f"System requirements for '{system}' are not available for '{game_name}'"}), 404

    return jsonify({
        "game": game_name,
        "system": system,
        "requirements": requirements
    })

@app.route('/games/<game_id>', methods=['PUT'])
def edit_game(game_id):
    data = request.get_json()

    game = database.games.find_one({"_id": int(game_id)})
    if not game:
        return jsonify({"error": f"Game with ID {game_id} not found"}), 404

    try:
        database.games.update_one({"_id": int(game_id)}, {"$set": data})
        return jsonify({"message": "Game updated successfully"}), 200
    except Exception as e:
        return jsonify({"error": f"Failed to update game: {str(e)}"}), 500

@app.route('/games/<game_id>', methods=['DELETE'])
def delete_game(game_id):
    game = database.games.find_one({"_id": int(game_id)})
    if not game:
        return jsonify({"error": f"Game with ID {game_id} not found"}), 404

    try:
        database.games.delete_one({"_id": int(game_id)})
        return jsonify({"message": f"Game with ID {game_id} deleted successfully"}), 200
    except Exception as e:
        return jsonify({"error": f"Failed to delete game: {str(e)}"}), 500

@app.route('/games', methods=['POST'])
def add_game():
    data = request.get_json()

    required_fields = ["name", "release_date", "developer", "platforms", "categories", "genres", "tags", "positive_ratings", "negative_ratings", "price", "detailed_description"]
    missing_fields = [field for field in required_fields if field not in data]

    if missing_fields:
        return jsonify({"error": f"Missing fields: {missing_fields}"}), 400

    try:
        database.games.insert_one(data)
        return jsonify({"message": "Game added successfully"}), 201
    except Exception as e:
        return jsonify({"error": f"Failed to add game: {str(e)}"}), 500


@app.route('/reports/top_genres_by_year', methods=['GET'])
def top_genres_by_year():
    pipeline = [
        {"$unwind": "$genres"},
        {
            "$addFields": {
                "release_year": {"$substr": ["$release_date", 0, 4]}
            }
        },
        {
            "$group": {
                "_id": {"year": "$release_year", "genre": "$genres"},
                "average_rating": {"$avg": "$positive_ratings"}
            }
        },
        {"$sort": {"average_rating": -1}},
        {"$limit": 10}
    ]
    result = list(database.games.aggregate(pipeline))
    return jsonify(result)



@app.route('/reports/developer_genre_ratings', methods=['GET'])
def developer_genre_ratings():
    pipeline = [
        {
            "$group": {
                "_id": {"developer": "$developer", "genre": "$genres"},
                "average_rating": {"$avg": "$positive_ratings"}
            }
        },
        {"$sort": {"_id.developer": 1, "_id.genre": 1}},
        {"$limit": 50}
    ]
    result = list(database.games.aggregate(pipeline))
    return jsonify(result)


@app.route('/games/bulk_update_price', methods=['PUT'])
def bulk_update_price():
    data = request.form or request.json
    developer = data.get('developer')
    discount_percentage = float(data.get('discount_percentage', 10)) / 100

    if not developer:
        return jsonify({"error": "Missing 'developer' parameter"}), 400

    print(f"Developer received: {developer}")

    games = list(
        database.games.find({"developer": {"$regex": f"^{developer}$", "$options": "i"}}, {"_id": 1, "price": 1}))
    print(f"Developer: {developer}, Games found: {len(games)}")

    if not games:
        return jsonify({"error": f"No games found for developer '{developer}'"}), 404

    for game in games:
        new_price = max(game['price'] * (1 - discount_percentage), 0.99)
        database.games.update_one({"_id": game["_id"]}, {"$set": {"price": new_price}})

    return jsonify({"message": f"Prices updated for {len(games)} games"}), 200

import webbrowser

@app.route('/games/<game_id>/header_img', methods=['GET'])
def open_header_img(game_id):
    game = database.games.find_one({"_id": int(game_id)}, {"_id": 0, "header_img": 1, "name": 1})
    if not game:
        return jsonify({"error": f"Game with ID {game_id} not found"}), 404

    header_img = game.get("header_img", "No Data Available")
    if header_img == "No Data Available":
        return jsonify({"error": f"Header image for game '{game_id}' is not available"}), 404

    webbrowser.open(header_img)
    return jsonify({"message": f"Opened header image for game '{game_id}' in the browser", "header_img": header_img}), 200


@app.route('/games/<game_id>/website', methods=['GET'])
def open_website(game_id):
    game = database.games.find_one({"_id": int(game_id)}, {"_id": 0, "website": 1, "name": 1})
    if not game:
        return jsonify({"error": f"Game with ID {game_id} not found"}), 404

    website = game.get("website", "No Data Available")
    if website == "No Data Available":
        return jsonify({"error": f"Website for game '{game_id}' is not available"}), 404

    webbrowser.open(website)
    return jsonify({"message": f"Opened website for game '{game_id}' in the browser", "website": website}), 200




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
                    "website",
                    "support_url",
                    "header_img",
                    "background_img",
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
                    "website": {
                        "bsonType": "string",
                        "description": "Url of the website about the game"
                    },
                    "support_url": {
                        "bsonType": "string",
                        "description": "Url to the steam support page"
                    },
                    "header_img": {
                        "bsonType": "string",
                        "description": "Url to the header image of the game."
                    },
                    "background_img": {
                        "bsonType": "string",
                        "description": "Url to the background image of the game."
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


    for _, record in s.iterrows():
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

    s=sources['steam_support_info.csv']
    website={}
    support_url={}
    for _, record in s.iterrows():
        appid = record['steam_appid']
        website[appid] = record['website']
        support_url[appid] = record['support_url']

    s=sources['steam_media_data.csv']
    header_img={}
    background_img={}
    for _,record in s.iterrows():
        appid = record['steam_appid']
        header_img[appid] = record['header_image']
        background_img[appid] = record['background']





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

        if record['appid'] in website.keys():
            game['website'] = str(website[record['appid']])
        else:
            game['website'] = 'No Data Available'
        if game['website']=="nan":
            game['website'] = "No Data Available"
        if record['appid'] in support_url.keys():
            game['support_url'] = str(support_url[record['appid']])
        else:
            game['support_url'] = 'No Data Available'
        if game['support_url']=="nan":
            game['support_url'] = "No Data Available"

        if record['appid'] in header_img.keys():
            game['header_img'] = str(header_img[record['appid']])
        else:
            game['header_img'] = "No Data Available"
        if record['appid'] in background_img.keys():
            game['background_img'] = str(background_img[record['appid']])
        else:
            game['background_img'] = "No Data Available"
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
