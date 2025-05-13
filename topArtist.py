import os
import pandas as pd
import spotipy
from spotipy.oauth2 import SpotifyOAuth
from sqlalchemy import create_engine

# Set your credentials
client_id = os.getenv('SPOTIFY_ID')
client_secret = os.getenv('SPOTIFY_SECRET')
redirect_uri = 'http://127.0.0.1:8888/callback'

# Set up Spotify OAuth
scope = 'user-top-read' 
sp_oauth = SpotifyOAuth(client_id=client_id, client_secret=client_secret, redirect_uri=redirect_uri, scope=scope, cache_path='.cache')

# Create a Spotipy client
sp = spotipy.Spotify(auth_manager=sp_oauth)

# Get current user's playlists
historical_artists_file = 'all_top_artists.json'
if os.path.exists(historical_artists_file):
    historical_artists_df = pd.read_json(historical_artists_file)
    all_artists = historical_artists_df.to_dict('records')
else:
    all_artists = []

# Get current user's top artists
top_artists = sp.current_user_top_artists(limit=50, time_range='medium_term')

# Create a list to hold new artist information
artist_info_ls = []

for artist in top_artists['items']:
    artist_info = {
        'Artistname': artist['name'],
        'Genre': artist['genres'] if artist['genres'] else ['Unknown Genre'],
        'FamousLevel': artist['popularity'],
        'Link': artist['external_urls']['spotify']
    }
    artist_info_ls.append(artist_info)

all_artists.extend(artist_info_ls)

unique_artists = {artist['Artistname']: artist for artist in all_artists}.values()

df = pd.DataFrame(artist_info_ls)
all_df = pd.DataFrame(unique_artists)

# Save to JSON with indentation
df.to_json("top_artists.json", orient="records", indent=4)
all_df.to_json("all_top_artists.json", orient="records", indent=4)

# Database connection parameters
username = os.getenv('POSTGRES_NAME') 
password = os.getenv('POSTGRES_PASSWORD')
host = os.getenv('POSTGRES_PASSWORD') 
port = os.getenv('POSTGRES_PORT')  
database_name = os.getenv('POSTGRES_DB') 

# Create a SQLAlchemy engine 
engine = create_engine(f'postgresql+psycopg2://{username}:{password}@{host}:{port}/{database_name}') 

# Load DataFrame into PostgreSQL table 
all_df.to_sql('all_top_artists', engine, if_exists='replace', index=False)

print("Top Artists Pipeline executed!")
