import os
import pandas as pd
import spotipy
from spotipy.oauth2 import SpotifyOAuth
from sqlalchemy import create_engine

# Set your credentials
client_id = 'e1579079c582472d998f11f720ae99e2'
client_secret = '5eecc48f72ca4d8fa9baecafb0e3b662'
redirect_uri = 'http://127.0.0.1:8888/callback'

# Set up Spotify OAuth
scope = 'user-read-recently-played'
sp_oauth = SpotifyOAuth(client_id=client_id, client_secret=client_secret, redirect_uri=redirect_uri, scope=scope, cache_path='.cache')

# Create a Spotipy client
sp = spotipy.Spotify(auth_manager=sp_oauth)

# Creating the recent tracks log
historical_tracks_file = 'all_recent_tracks.json'

# Checks if the file exists 
if os.path.exists(historical_tracks_file):
    historical_tracks_df = pd.read_json(historical_tracks_file)
    tracks_ls = historical_tracks_df.to_dict('records')
else:
    tracks_ls = []

# Get current user's playlists
recent_tracks = sp.current_user_recently_played(limit=50)

# Creating the recent tracks list
recent_tracks_ls = []
for item in recent_tracks['items']:
    track_name = item['track']['name']
    artist_name = item['track']['artists'][0]['name']
    played_at = item['played_at']
    
    recent_tracks_ls.append({
        'track_name': track_name,
        'artist_name': artist_name,
        'played_at': played_at
    })

tracks_ls.extend(recent_tracks_ls)
unique_tracks = {track['played_at']: track for track in tracks_ls}.values()

df = pd.DataFrame(recent_tracks_ls)
track_df = pd.DataFrame(unique_tracks)
track_df['played_at'] = pd.to_datetime(track_df['played_at'], utc=True)
track_df.sort_values(by ='played_at', inplace=True)

df.to_json("recent_tracks.json", orient="records", indent=4)
track_df.to_json("all_recent_tracks.json", orient="records", indent=4)


#Now loading the data into the postgreSQL database.
# Database connection parameters
username = 'ifeoma' 
password = 'postgre12' 
host = 'localhost' 
port = '5432'  
database_name = 'spotifydata2' 

# Create a SQLAlchemy engine 
engine = create_engine(f'postgresql+psycopg2://{username}:{password}@{host}:{port}/{database_name}') 

# Load DataFrame into PostgreSQL table 
track_df.to_sql('all_recent_tracks', engine, if_exists='replace', index=False)

print("Played Pipeline executed!")