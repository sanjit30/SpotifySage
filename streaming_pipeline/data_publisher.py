import requests
import time
from spotipy import Spotify
from spotipy.oauth2 import SpotifyClientCredentials
from confluent_kafka import Producer, KafkaException
from dotenv import load_dotenv
import os
import json
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from config.data_models import Base, ReleaseDimension, PerformerDimension, TrackDimension
from sqlalchemy.exc import OperationalError
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

def load_message_config():
    """Read message broker configuration"""
    config_settings = {
        'bootstrap.servers': 'message_broker:29092',
        'acks': 'all',
        'retries': 3,
        'batch.size': 16384,
        'linger.ms': 1
    }
    
    try:
        with open("client.properties") as config_file:
            for config_line in config_file:
                config_line = config_line.strip()
                if len(config_line) != 0 and config_line[0] != "#":
                    config_key, config_value = config_line.strip().split('=', 1)
                    config_settings[config_key] = config_value.strip()
    except FileNotFoundError:
        logger.warning("Configuration file 'client.properties' not found. Using defaults.")
    except Exception as e:
        logger.error(f"Error reading configuration file: {e}")
    
    return config_settings

def get_performer_info(performer_id: str) -> dict:
    """Fetch performer data from Spotify API"""
    try:
        response_data = spotify_service.artist(performer_id)
    except requests.exceptions.ReadTimeout:
        logger.warning(f"Timeout occurred for performer_id: {performer_id}. Retrying...")
        response_data = spotify_service.artist(performer_id)
    
    performer_data = {
        "performer_key": performer_id,
        "performer_name": response_data['name'],
        "external_link": response_data['external_urls']['spotify'],
        "followers_total": response_data['followers']['total'],
        "profile_image": response_data['images'][0]['url'] if response_data['images'] else '',
        "popularity_index": response_data['popularity'],
    }
    return performer_data

def get_release_info(release_id: str) -> dict:
    """Fetch release data from Spotify API"""
    try:
        response_data = spotify_service.album(release_id)
    except requests.exceptions.ReadTimeout:
        logger.warning(f"Timeout occurred for release_id: {release_id}. Retrying...")
        response_data = spotify_service.album(release_id)
    
    release_data = {
        "release_key": release_id,
        "release_name": response_data['name'],
        "track_count": response_data['total_tracks'],
        "publish_date": response_data['release_date'],
        "external_link": response_data['external_urls']['spotify'],
        "cover_image": response_data['images'][0]['url'] if response_data['images'] else '',
        "publisher_name": response_data.get('label', 'Unknown'),
        "popularity_index": response_data['popularity'],
    }
    return release_data

def get_track_info(track_id: str) -> dict:
    """Fetch track data from Spotify API"""
    try:
        response_data = spotify_service.track(track_id)
    except requests.exceptions.ReadTimeout:
        logger.warning(f"Timeout occurred for track_id: {track_id}. Retrying...")
        response_data = spotify_service.track(track_id)
    
    track_data = {
        "track_key": track_id,
        "track_name": response_data['name'],
        "disc_position": response_data['disc_number'],
        "length_milliseconds": response_data['duration_ms'],
        "contains_explicit": response_data['explicit'],
        "external_link": response_data['external_urls']['spotify'],
        "sample_url": response_data.get('preview_url', 'No preview available'),
        "popularity_index": response_data['popularity'],
    }
    return track_data

def send_to_message_broker(message_data, config_settings, topic_name):
    """Send data to message broker topic"""
    message_producer = Producer(config_settings)
    
    def delivery_notification(error, message):
        if error:
            logger.error(f'ERROR: Message delivery failed: {error}')
        else:
            message_key = message.key().decode('utf-8') if message.key() else None
            message_value = message.value().decode('utf-8') if message.value() else None
            logger.info(f"Published event to topic {message.topic()}: key = {message_key}")
    
    try:
        message_producer.produce(topic_name, value=json.dumps(message_data).encode('utf-8'), callback=delivery_notification)
        message_producer.flush()
        logger.info(f"Published data to topic {topic_name}")
    except KafkaException as e:
        logger.error(f"Error publishing to message broker: {e}")

if __name__ == '__main__':
    config_settings = load_message_config()
    
    # Spotify initialization
    app_id = os.getenv('SPOTIFY_APP_ID')
    app_secret = os.getenv('SPOTIFY_APP_SECRET')
    
    if not app_id or not app_secret:
        logger.error("Spotify credentials not found in environment variables")
        exit(1)
    
    spotify_service = Spotify(client_credentials_manager=SpotifyClientCredentials(
        client_id=app_id, 
        client_secret=app_secret
    ))
    
    # Database initialization
    db_user = os.getenv("DATABASE_USER", "streaming_user")
    db_password = os.getenv("DATABASE_PASSWORD", "streaming_pass_2025")
    db_host = os.getenv("DATABASE_HOST", "data_warehouse")
    db_name = os.getenv("DATABASE_NAME", "music_streaming_db")
    database_url = f"postgresql+psycopg2://{db_user}:{db_password}@{db_host}:5432/{db_name}"
    
    logger.info(f"Connecting to database: {db_host}:{db_name}")
    
    try:
        db_engine = create_engine(database_url)
        DatabaseSession = sessionmaker(autocommit=False, autoflush=False, bind=db_engine)
        
        # Create all tables if they don't exist
        Base.metadata.create_all(db_engine)
        logger.info("Database tables created/verified successfully")
        
        # Create a database session
        db_session = DatabaseSession()
        
        # API endpoint initialization
        api_endpoint = 'http://web_api:5000'
        last_check_time = int(time.time()) * 1000
        
        logger.info("Starting data publisher loop...")
        
        while True:
            try:
                current_time = int(time.time()) * 1000
                request_payload = {'after_timestamp': last_check_time}
                
                try:
                    logger.info("Fetching recent tracks from web API...")
                    api_response = requests.post(api_endpoint, json=request_payload, timeout=30).json()
                    
                    if api_response['status_code'] == 204:
                        logger.info("No new listening data available")
                    elif api_response['status_code'] == 200:
                        logger.info(f"Processing {len(api_response['listening_events'])} new events")
                        
                        for listening_event in api_response['listening_events']:
                            # Check if track exists in database
                            existing_track = db_session.query(TrackDimension).filter_by(track_key=listening_event['track_key']).first()
                            if not existing_track:
                                logger.info(f"Fetching track data for: {listening_event['track_key']}")
                                send_to_message_broker(get_track_info(listening_event['track_key']), config_settings, 'track_data_topic')
                            
                            # Check if release exists in database
                            existing_release = db_session.query(ReleaseDimension).filter_by(release_key=listening_event['release_key']).first()
                            if not existing_release:
                                logger.info(f"Fetching release data for: {listening_event['release_key']}")
                                send_to_message_broker(get_release_info(listening_event['release_key']), config_settings, 'release_data_topic')
                            
                            # Check if performer exists in database
                            existing_performer = db_session.query(PerformerDimension).filter_by(performer_key=listening_event['performer_key']).first()
                            if not existing_performer:
                                logger.info(f"Fetching performer data for: {listening_event['performer_key']}")
                                send_to_message_broker(get_performer_info(listening_event['performer_key']), config_settings, 'performer_data_topic')
                            
                            # Process listening event
                            event_info = {
                                'track_key': listening_event['track_key'],
                                'release_key': listening_event['release_key'],
                                'performer_key': listening_event['performer_key'],
                                'timestamp_played': listening_event['timestamp_played']
                            }
                            
                            logger.info("Publishing listening event data")
                            send_to_message_broker(event_info, config_settings, 'listening_events_topic')
                    
                    else:
                        logger.warning(f"Unexpected response from web API: {api_response}")
                
                except requests.exceptions.RequestException as e:
                    logger.error(f"Request to web API failed: {e}")
                except OperationalError as e:
                    logger.error(f"Database operation failed: {e}")
                    db_session.rollback()
                
                last_check_time = current_time
                logger.info("Sleeping for 60 seconds...")
                time.sleep(60)
                
            except KeyboardInterrupt:
                logger.info("Process interrupted by user.")
                break
            except Exception as e:
                logger.error(f"Unexpected error in publisher loop: {e}")
                time.sleep(60)  # Wait before retrying
    
    except Exception as e:
        logger.error(f"Failed to initialize data publisher: {e}")
    finally:
        try:
            db_session.close()
            db_engine.dispose()
            logger.info("Database connections closed successfully")
        except Exception as e:
            logger.error(f"Error closing database connections: {e}")