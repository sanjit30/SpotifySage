import uuid
import json
from flask import Flask, redirect, session, request, url_for, jsonify
from spotipy import Spotify, SpotifyOAuth
import os
from dotenv import load_dotenv
import secrets
import time
from datetime import datetime
import logging
from config.helper_functions import generate_weekly_insights, generate_daily_insights
from config.helper_functions import create_database_connection
from config.helper_functions import generate_ollama_playlist

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Flask app setup
webapp = Flask(__name__)
webapp.secret_key = secrets.token_hex(16)

# Spotify settings
SPOTIFY_APP_ID = os.getenv('SPOTIFY_APP_ID')
SPOTIFY_APP_SECRET = os.getenv('SPOTIFY_APP_SECRET')
ACCESS_TOKEN_DATA = 'access_token_data'

def setup_spotify_auth():
    """Create Spotify OAuth handler"""
    return SpotifyOAuth(
        client_id=SPOTIFY_APP_ID,
        client_secret=SPOTIFY_APP_SECRET,
        redirect_uri=url_for('callback_handler', _external=True),
        scope='user-read-recently-played playlist-modify-public playlist-modify-private'
    )

def retrieve_access_token():
    """Get valid Spotify access token"""
    token_data = load_access_token()
    if not token_data:
        logger.info('No token data found, redirecting to login')
        return redirect(url_for('spotify_authentication', _external=True))
    
    # Check if token needs refresh
    token_expired = token_data['expires_at'] - int(time.time()) < 600
    if token_expired:
        logger.info('Token expired, refreshing...')
        spotify_auth = setup_spotify_auth()
        token_data = spotify_auth.refresh_access_token(token_data['refresh_token'])
        store_access_token(token_data)
    
    return token_data['access_token']

def store_access_token(token_data):
    """Save token data to file"""
    try:
        with open('/app/api_service/access_token.json', 'w') as token_file:
            token_file.write(json.dumps(token_data))
        logger.info('Token data saved successfully')
    except Exception as e:
        logger.error(f'Error saving token data: {e}')

def load_access_token():
    """Load token data from file"""
    try:
        with open('/app/api_service/access_token.json', 'r') as token_file:
            token_content = json.load(token_file)
            return token_content
    except FileNotFoundError:
        logger.warning('Token file not found')
        return None
    except KeyError:
        logger.warning('Invalid token file format')
        return None
    except Exception as e:
        logger.error(f'Error loading token data: {e}')
        return None

@webapp.route('/', methods=['GET'])
def homepage():
    """Home page"""
    return {
        "message": "Music Streaming Analysis API",
        "endpoints": [
            "/spotify_login - Authenticate with Spotify",
            "/ (POST) - Stream listening data", 
            "/weekly_insights - Generate weekly summary",
            "/create_playlist (POST) - Create daily playlist"
        ]
    }

@webapp.route('/', methods=['POST'])
def stream_listening_data():
    """Stream listening data endpoint"""
    try:
        # Get valid token
        access_token = retrieve_access_token()
        if isinstance(access_token, dict) and 'url' in str(access_token):
            return access_token
        
        spotify_client = Spotify(auth=access_token)
        request_payload = request.get_json()
        
        if not request_payload or 'after_timestamp' not in request_payload:
            return {
                "status_code": 400,
                "message": "Missing 'after_timestamp' parameter in request body"
            }, 400
        
        # Get recently played tracks
        recent_listening = spotify_client.current_user_recently_played(after=request_payload['after_timestamp'])
        
        if not recent_listening['items']:
            return {
                "status_code": 204,
                "message": "No new listening data"
            }
        
        listening_events = []
        for listening_item in recent_listening['items']:
            timestamp_played = listening_item['played_at']
            track_key = listening_item['track']['id']
            release_key = listening_item['track']['album']['id']
            performer_key = listening_item['track']['artists'][0]['id']
            
            listening_events.append({
                'event_id': uuid.uuid4().hex,
                'timestamp_played': timestamp_played,
                'track_key': track_key,
                'release_key': release_key,
                'performer_key': performer_key,
            })
        
        logger.info(f"Retrieved {len(listening_events)} new listening events")
        return {
            'status_code': 200,
            'message': f'{len(listening_events)} new tracks',
            'listening_events': listening_events,
        }
    
    except Exception as e:
        logger.error(f'Error in stream_listening_data: {e}')
        return {
            "status_code": 500,
            "message": "Internal Server Error"
        }, 500

@webapp.route('/weekly_insights', methods=['GET'])
def weekly_insights():
    """Generate AI insights from listening history"""
    try:
        logger.info('Generating weekly insights...')
        analysis_result = generate_weekly_insights()
        
        if analysis_result != 'Success':
            return {
                "status_code": 500,
                "message": f"Internal Server Error: {analysis_result}"
            }, 500
        
        return {
            "status_code": 200,
            "message": "Success. Check your email for the insights"
        }
    
    except Exception as e:
        logger.error(f'Error in weekly_insights: {e}')
        return {
            "status_code": 500,
            "message": f"Internal Server Error: {str(e)}"
        }, 500
    
@webapp.route('/daily_insights', methods=['GET'])
def daily_insights():
    """Generate daily insights from listening history"""
    try:
        logger.info('Generating daily insights...')
        analysis_result = generate_daily_insights()
        
        if analysis_result != 'Success':
            return {
                "status_code": 500,
                "message": f"Internal Server Error: {analysis_result}"
            }, 500
        
        return {
            "status_code": 200,
            "message": "Success. Check your email for the insights"
        }
    
    except Exception as e:
        logger.error(f'Error in weekly_insights: {e}')
        return {
            "status_code": 500,
            "message": f"Internal Server Error: {str(e)}"
        }, 500
    

@webapp.route('/ollama_playlist', methods=['POST'])
def ollama_playlist():
    try:
        access_token = retrieve_access_token()
        if isinstance(access_token, dict) and 'url' in str(access_token):
            return access_token  # Redirect or error response

        spotify_client = Spotify(auth=access_token)
        user_id = spotify_client.current_user()['id']

        connection = create_database_connection()
        cursor = connection.cursor()

        playlist_url = generate_ollama_playlist(spotify_client, user_id, cursor)

        cursor.close()
        connection.close()

        if playlist_url:
            return jsonify({
                "status": "success",
                "message": "Playlist created successfully",
                "playlist_url": playlist_url
            })
        else:
            return jsonify({
                "status": "fail",
                "message": "Failed to create playlist"
            }), 500

    except Exception as e:
        logging.error(f"Error in /ollama_playlist: {e}")
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500
    

@webapp.route('/spotify_login', methods=['GET'])
def spotify_authentication():
    """Spotify login endpoint"""
    try:
        authorization_url = setup_spotify_auth().get_authorize_url()
        logger.info('Redirecting to Spotify authorization')
        return redirect(authorization_url)
    except Exception as e:
        logger.error(f'Error in spotify_authentication: {e}')
        return {
            "status_code": 500,
            "message": f"Login error: {str(e)}"
        }, 500

@webapp.route('/callback')
def callback_handler():
    """Spotify OAuth callback handler"""
    try:
        session.clear()
        authorization_code = request.args.get('code')
        if not authorization_code:
            return {
                "status_code": 400,
                "message": "No authorization code received"
            }, 400
        
        token_data = setup_spotify_auth().get_access_token(authorization_code)
        store_access_token(token_data)
        session[ACCESS_TOKEN_DATA] = token_data
        
        logger.info('Successfully authenticated with Spotify')
        return {
            "status_code": 200,
            "message": "Successfully authenticated with Spotify! You can now use the API endpoints."
        }
    
    except Exception as e:
        logger.error(f'Error in callback_handler: {e}')
        return {
            "status_code": 500,
            "message": f"Authentication error: {str(e)}"
        }, 500

@webapp.route('/health_status', methods=['GET'])
def health_status():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "timestamp": datetime.now().isoformat()
    }

if __name__ == '__main__':
    logger.info('Starting Flask web application...')
    webapp.run(host='0.0.0.0', port=5000, debug=True)