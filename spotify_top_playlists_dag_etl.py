#The following codes extract top playlists from spotify daily, clean them, and loads in a mysql AWS RDS database
#To extract the data from the spotify API you will need to create a spotify account if you don't have one, and use 'spotify for developers' to the
#       clientID and secretID to be authorized to use the API
#You interact with the spotify API via the spotipy module
#And to load the data you will need an AWS account, and then create an RDS database (here I use mysql)
# And for the process to run daily, you will need a scheduling tool (here I use airflow via the airflow library)

import pandas as pd
import numpy as np
import json
import spotipy
import spotipy.util as util
from json.decoder import JSONDecodeError
from spotipy.oauth2 import SpotifyClientCredentials
from spotipy import Spotify
import pymysql

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from datetime import timedelta
from datetime import datetime

def spotify_top_playlists():
    client = "ReplaceWithYourClientId"
    secret=  "AndYourSecretID"

    client_credentials = SpotifyClientCredentials(client_id = client, client_secret = secret)
    spotifyObject = spotipy.Spotify(client_credentials_manager = client_credentials)

    ft_playlists = spotifyObject.featured_playlists(limit = 50)
    parse = json.dumps(ft_playlists, sort_keys = True, indent = 4)


    playlist_name = []
    collaborative = []
    playlist_id = []
    public = []
    number_of_tracks = []
    types = []
    primary_color = []


    for song in ft_playlists["playlists"]['items']:
        playlist_name.append(song['name'])
        collaborative.append(song['collaborative'])
        playlist_id.append(song['id'])
        public.append(song["public"])
        number_of_tracks.append(song["tracks"]["total"])
        types.append(song["type"])
        primary_color.append(song["primary_color"])
    
    
    weekly_playlists = {
        'id': playlist_id,
        'name': playlist_name,
        'public': public,
        'number of tracks': number_of_tracks,
        'types': types}  

    playlist_df = pd.DataFrame(weekly_playlists, columns = ['id', 'name', 'number of tracks'])
    print(playlist_df)

    host = 'insertYourDatabaseEndpoint'
    user = 'admin'
    password = 'useyourpassword'
    database = 'andyourdatabase'

    connection = pymysql.connect(host= host,
                            user= user,
                            password= password,
                            db= database)

    cursor=connection.cursor()

    for i,row in playlist_df.iterrows():
        sql = "INSERT INTO `best_playlists` (playlist_id, playlist_name, total_tracks) VALUES (" + "%s,"*(len(row)-1) + "%s)"
        cursor.execute(sql, tuple(row))
        connection.commit()

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2022,8,8),
    'email': ['bnthierry2@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes = 1)

}

my_dag = DAG('spotify_dag', 
default_args = default_args, 
description = 'top playlists dag', 
schedule_interval= timedelta(days=1))


etl = PythonOperator(
    task_id = 'top_playlists',
    python_callable= spotify_top_playlists,
    dag = my_dag

)

etl


