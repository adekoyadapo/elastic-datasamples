import requests
import json
from datetime import datetime, timedelta
import os
from elasticsearch import Elasticsearch, helpers
import logging
from dotenv import load_dotenv

load_dotenv()

# Initialize Elasticsearch client
def initialize_elasticsearch():
    try:
        cloud_id = os.getenv("ELASTIC_CLOUD_ID")
        if cloud_id:
            es = Elasticsearch(cloud_id=cloud_id, api_key=os.getenv("ELASTIC_API_KEY"))
        else:
            es = Elasticsearch(
                hosts=[{
                    "host": os.getenv("ELASTIC_HOST", "http://localhost"),
                    "port": int(os.getenv("ELASTIC_PORT", 9200)),
                    "scheme": os.getenv("HTTP_SCHEME", "http")
                }],
                basic_auth=(
                    os.getenv("ELASTIC_USERNAME", "elastic"),
                    os.getenv("ELASTIC_PASSWORD", "changeme"),
                ),
                ssl_show_warn=False,
                verify_certs=False
            )
        return es
    except Exception as e:
        logging.error(f"Error initializing Elasticsearch client: {e}")
        raise

# Bulk index function
def bulk_index_data(es, bulk_data):
    try:
        helpers.bulk(es, bulk_data)
        print("Bulk data indexed successfully.")
    except Exception as e:
        logging.error(f"Error during bulk indexing: {e}")

# Set up parameters
target_index = 'nhl'
mappings = {
    "mappings": {
        "properties": {
            "timestamp": { "type": "date" },
            "teamid": { "type": "integer" },
            "teamnick": { "type": "keyword" },
            "teamnick_opposing": { "type": "keyword" },
            "play_id": { "type": "integer" },
            "period": { "type": "integer" },
            "time": { "type": "keyword" },
            "description": { "type": "keyword" },
            "game": {
                "properties": {
                    "id": { "type": "keyword" },
                    "home_team": { "type": "keyword" },
                    "away_team": { "type": "keyword" },
                    "date": { "type": "date" },
                    "location": { "type": "keyword" }
                }
            }
        }
    }
}

total_plays = 0
arg_season = input("Enter the season (e.g., 2024): ")
arg_game = input("Enter the game ID (optional): ")
arg_end_date = input("Enter the end date in number of days (default 1 year): ")
season_start_date = f"{arg_season}-01-01"
es = initialize_elasticsearch()

bulk_data = []
current_date = datetime.strptime(season_start_date, "%Y-%m-%d")
end_date = current_date + timedelta(days=int(arg_end_date or 365))

while current_date < end_date:
    schedule_url = f"https://api-web.nhle.com/v1/schedule/{current_date.strftime('%Y-%m-%d')}"
    response = requests.get(schedule_url)
    
    if response.status_code != 200:
        print(f"Error retrieving schedule for {current_date.strftime('%Y-%m-%d')}: {response.text}")
        current_date += timedelta(days=1)
        continue
    
    schedule_data = response.json()
    game_weeks = schedule_data.get("gameWeek", [])

    for week in game_weeks:
        games = week.get("games", [])
        for game in games:
            game_id = game.get("id")
            game_date = datetime.strptime(week.get("date"), "%Y-%m-%d")
            
            if arg_game and game_id != int(arg_game):
                continue

            print(f"Processing game {game_id} on {game_date}")
            
            play_url = f"https://api-web.nhle.com/v1/gamecenter/{game_id}/play-by-play"
            play_response = requests.get(play_url)

            if play_response.status_code != 200:
                print(f"Error retrieving play-by-play for game {game_id}: {play_response.text}")
                continue

            play_data = play_response.json()
            plays = play_data.get("plays", [])

            for play in plays:
                play_id = play.get("eventId")
                period_number = play["periodDescriptor"].get("number")
                time_in_period = play.get("timeInPeriod")
                play_type = play.get("typeDescKey")
                details = play.get("details", {})
                
                home_team = play_data.get("homeTeam", {}).get("name", {}).get("default", "Home Team")
                away_team = play_data.get("awayTeam", {}).get("name", {}).get("default", "Away Team")
                
                play_minutes = int(time_in_period[:2]) + ((period_number - 1) * 20)
                play_seconds = int(time_in_period[3:])
                play_timestamp = game_date + timedelta(minutes=play_minutes, seconds=play_seconds)
                
                play_doc = {
                    "timestamp": play_timestamp.isoformat(),
                    "teamid": details.get("eventOwnerTeamId"),
                    "teamnick": home_team if details.get("eventOwnerTeamId") == play_data.get("homeTeam", {}).get("id") else away_team,
                    "teamnick_opposing": away_team if details.get("eventOwnerTeamId") == play_data.get("homeTeam", {}).get("id") else home_team,
                    "play_id": play_id,
                    "period": period_number,
                    "time": time_in_period,
                    "description": play_type,
                    "game": {
                        "id": game_id,
                        "home_team": home_team,
                        "away_team": away_team,
                        "date": game_date.strftime("%Y-%m-%d"),
                        "location": play_data.get("venue", {}).get("default", "Venue")
                    }
                }

                total_plays += 1
                bulk_data.append({
                    "_op_type": "index",
                    "_index": target_index,
                    "_id": f"{game_id}:{play_timestamp.isoformat()}",
                    "_source": play_doc
                })

    current_date += timedelta(days=1)

if bulk_data:
    bulk_index_data(es, bulk_data)

print(f"Total Plays Read: {total_plays}")