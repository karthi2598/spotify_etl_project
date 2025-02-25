from datetime import datetime, timedelta
from airflow import DAG 
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python_operator import PythonOperator
import os

def get_spotify_data(ti):
    client_id = os.getenv("SPOTIFY_CLIENT_ID")
    client_secret = os.getenv("SPOTIFY_CLIENT_SECRET")

    client_credentials_manager = SpotifyClientCredentials(client_id=client_id, client_secret=client_secret)
    sp = spotipy.Spotify(auth_manager=client_credentials_manager)

    # Make the API request
    artist_name = "Taylor Swift"
    results = sp.search(q=artist_name, type='track', limit=10)

    tracks = []
    for item in results['tracks']['items']:
        track_data = {
            'track_name': item['name'],
            'artist_name': item['artists'][0]['name'],
            'popularity': item['popularity']
        }
        tracks.append(track_data)

    ti.xcom_push(key='spotify_tracks', value=tracks)

def transform_data(ti):
    tracks = ti.xcom_pull(task_ids='extract', key='spotify_tracks')  # Assign it properly
    transformed_tracks = [track for track in tracks if track['popularity'] > 50]  # Correct filtering

    ti.xcom_push(key='transformed_tracks', value=transformed_tracks)


def load_data(ti):
    transformed_tracks = ti.xcom_pull(task_ids='transform', key='transformed_tracks')  # Correct key
    try:
        pg_hook = PostgresHook(postgres_conn_id='spotify_connection')  # Fix typo
        conn = pg_hook.get_conn()
        cur = conn.cursor()
        cur.execute("""
                    CREATE TABLE IF NOT EXISTS spotify_tracks(
                        track_name TEXT,
                        artist_name TEXT,
                        popularity INT)
                    """)
        for track in transformed_tracks:
            cur.execute("""
                        INSERT INTO spotify_tracks (track_name, artist_name, popularity)
                        VALUES (%s, %s, %s)
                        """, (track['track_name'], track['artist_name'], track['popularity']))
        conn.commit()
        cur.close()
        conn.close()
        print("Data loaded successfully!")
    except Exception as e:
        print(f"Error loading data: {e}")



default_args = {
    'owner':'airflow',
    'start_date': datetime(2025,1,1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    }

dag = DAG(
    'ETL_Spotify_data',
    description = 'A simple DAG to extract transform and load spotify data',
    default_args = default_args,
    schedule_interval=timedelta(days=1),
    )
dag.catchup = False

extract_task = PythonOperator(
    task_id='extract',
    python_callable = get_spotify_data,
    provide_context=True,  # Passes context to the function so we can access XCom
    dag=dag
)

transform_task = PythonOperator(
    task_id='transform',
    python_callable=transform_data,
    provide_context=True,  # Passes context to the function so we can access XCom
    dag=dag
)

load_task = PythonOperator(
    task_id='load',
    python_callable=load_data,
    provide_context=True,  # Passes context to the function so we can access XCom
    dag=dag
)

extract_task >> transform_task >> load_task