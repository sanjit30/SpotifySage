import html
import json
import psycopg2
import os
from dotenv import load_dotenv
import ollama
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import logging
import datetime
import time

# Import Spotify class for type hinting
from spotipy import Spotify

load_dotenv()
os.environ['OLLAMA_HOST'] = os.getenv('OLLAMA_ENDPOINT', 'http://ollama_service:11434')

def safe_html(text):
    """Escape text for safe HTML rendering."""
    if not text:
        return ""
    return html.escape(text).replace('\n', '<br>')

def create_database_connection():
    """Create database connection"""
    try:
        connection = psycopg2.connect(
            dbname=os.getenv('DATABASE_NAME', 'music_streaming_db'),
            user=os.getenv('DATABASE_USER', 'streaming_user'),
            password=os.getenv('DATABASE_PASSWORD', 'streaming_pass_2025'),
            host=os.getenv('DATABASE_HOST', 'data_warehouse'),
            port=5432
        )
        return connection
    except Exception as e:
        print(f"Database connection error: {e}")
        raise


def extract_json(s: str):
    """Extract the first complete JSON object or array from string s."""
    stack = 0
    start_idx = None
    opening_char = None
    for i, char in enumerate(s):
        if char in ['{', '[']:
            if stack == 0:
                start_idx = i
                opening_char = char
            stack += 1
        elif char in ['}', ']']:
            stack -= 1
            if stack == 0 and start_idx is not None:
                return s[start_idx:i+1]
    return None


def safe_html(text):
    """Escape text for safe HTML rendering."""
    if not text:
        return ""
    return html.escape(text).replace('\n', '<br>')



def generate_ollama_playlist(spotify_client: Spotify, user_id: str, db_cursor):
    logging.info("Fetching last 24h listened tracks from database...")
    db_cursor.execute("""
        SELECT DISTINCT td.track_name, pd.performer_name
        FROM listening_fact lf
        JOIN track_dimension td ON lf.track_key = td.track_key
        JOIN performer_dimension pd ON lf.performer_key = pd.performer_key
        WHERE lf.timestamp_played >= (CURRENT_TIMESTAMP - INTERVAL '24 HOURS')
    """)
    tracks = db_cursor.fetchall()
    print(tracks , " : tracks fetched from DB")  # For debugging purposes; can be removed in prod
    logging.info(f"Found {len(tracks)} unique tracks listened in last 24 hours.")

    if not tracks:
        logging.warning("No recent listening data to generate recommendations.")
        return None

    prompt = "These are the songs the user listened to in the last day:\n"
    for name, artist in tracks:
        prompt += f"- '{name}' by {artist}\n"
    prompt += (
        "\nPlease recommend 15 songs that complement these songs. "
        "Reply only with a JSON array of objects in the format [{\"track\": \"Song Title\", \"artist\": \"Artist Name\"}]."
    )

    system_prompt = "You are a helpful and accurate music recommendation AI."

    try:
        response = ollama.chat(
            model="llama3.2",
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": prompt},
            ],
        )
        raw_content = response['message']['content']
        logging.info("Received response from Ollama.")
        print(raw_content)  # For debugging purposes; can be removed in prod

        if raw_content.startswith("```"):
            # Remove Markdown triple backticks if present
            raw_content = "\n".join(raw_content.split("\n")[1:-1]).strip()

        json_text = extract_json(raw_content)

        print(json_text)  # For debugging purposes; can be removed in prod
        if not json_text:
            logging.error("No complete JSON object or array found in Ollama response.")
            return None

        recommendations = json.loads(json_text)
        print(recommendations)  # For debugging purposes; can be removed in prod

        # Normalize recommendations to list
        if isinstance(recommendations, dict):
            recommendations = [recommendations]

        logging.info(f"Ollama returned {len(recommendations)} recommendations.")

    except Exception as ex:
        logging.error(f"Error fetching/parsing Ollama response: {ex}")
        return None

    recommended_ids = []

    for item in recommendations:
        if isinstance(item, dict):
            track_name = item.get('track', '').strip()
            artist_name = item.get('artist', '').strip()
        elif isinstance(item, str):
            # If only track names are present, no artist info
            track_name = item.strip()
            artist_name = ''
        else:
            logging.warning(f"Skipping invalid recommendation item: {item}")
            continue

        if not track_name:
            logging.warning(f"Skipping recommendation with empty track name: {item}")
            continue

        query = f"track:{track_name}"
        if artist_name:
            query += f" artist:{artist_name}"

        results = spotify_client.search(query, type="track", limit=1)
        items = results.get('tracks', {}).get('items', [])

        if items:
            track_info = items[0]
            track_title = track_info['name']
            artists = ", ".join([a['name'] for a in track_info['artists']])
            logging.info(f"Matched '{track_title}' by {artists} on Spotify.")
            recommended_ids.append(track_info['id'])
        else:
            logging.warning(f"No Spotify match for query: {query}")

    if not recommended_ids:
        logging.warning("No tracks matched on Spotify for the recommended songs.")
        return None

    playlist_name = f"Ollama Recommendations {datetime.datetime.now().strftime('%Y-%m-%d')}"
    playlist = spotify_client.user_playlist_create(
        user=user_id,
        name=playlist_name,
        public=False,
        description="Playlist generated by AI recommendations based on your recent listens."
    )
    spotify_client.user_playlist_add_tracks(user=user_id, playlist_id=playlist['id'], tracks=recommended_ids)

    logging.info(f"Created playlist '{playlist_name}' with {len(recommended_ids)} tracks.")
    return playlist['external_urls']['spotify']

def generate_weekly_insights():
    """Generate weekly listening insights and send email with only query results"""
    try:
        connection = create_database_connection()
        db_cursor = connection.cursor()
        data_queries = [
            {
                'section_name': 'Top 10 Tracks',
                'sql_query': """
                    SELECT td.track_name, rd.release_name, pd.performer_name, rd.cover_image,
                           FLOOR(SUM(td.length_milliseconds) / (1000 * 60)) AS minutes_total
                    FROM listening_fact lf
                    JOIN track_dimension td ON lf.track_key = td.track_key
                    JOIN release_dimension rd ON lf.release_key = rd.release_key
                    JOIN performer_dimension pd ON lf.performer_key = pd.performer_key
                    WHERE lf.timestamp_played >= date_trunc('week', CURRENT_DATE) + INTERVAL '1 minute'
                    AND lf.timestamp_played < date_trunc('week', CURRENT_DATE) + INTERVAL '6 days 8 hours 59 minutes'
                    GROUP BY td.track_name, rd.release_name, pd.performer_name, rd.cover_image
                    ORDER BY minutes_total DESC
                    LIMIT 10;
                """
            },
            {
                'section_name': 'Top 5 Releases',
                'sql_query': """
                    SELECT rd.release_name, pd.performer_name, rd.cover_image,
                           FLOOR(SUM(td.length_milliseconds) / (1000 * 60)) AS minutes_total
                    FROM listening_fact lf
                    JOIN release_dimension rd ON lf.release_key = rd.release_key
                    JOIN performer_dimension pd ON lf.performer_key = pd.performer_key
                    JOIN track_dimension td ON lf.track_key = td.track_key
                    WHERE lf.timestamp_played >= date_trunc('week', CURRENT_DATE) + INTERVAL '1 minute'
                    AND lf.timestamp_played < date_trunc('week', CURRENT_DATE) + INTERVAL '6 days 8 hours 59 minutes'
                    GROUP BY rd.release_name, pd.performer_name, rd.cover_image
                    ORDER BY minutes_total DESC
                    LIMIT 5;
                """
            },
            {
                'section_name': 'Top 5 Performers',
                'sql_query': """
                    SELECT pd.performer_name,
                           FLOOR(SUM(td.length_milliseconds) / (1000 * 60)) AS minutes_total
                    FROM listening_fact lf
                    JOIN performer_dimension pd ON lf.performer_key = pd.performer_key
                    JOIN track_dimension td ON lf.track_key = td.track_key
                    WHERE lf.timestamp_played >= date_trunc('week', CURRENT_DATE) + INTERVAL '1 minute'
                    AND lf.timestamp_played < date_trunc('week', CURRENT_DATE) + INTERVAL '6 days 8 hours 59 minutes'
                    GROUP BY pd.performer_name
                    ORDER BY minutes_total DESC
                    LIMIT 5;
                """
            },
            {
                'section_name': 'Tracks not heard recently',
                'sql_query': """
                    SELECT td.track_name, rd.cover_image, MAX(lf.timestamp_played) AS last_play,
                           (CURRENT_DATE - MAX(lf.timestamp_played)::date) AS days_since
                    FROM listening_fact lf
                    JOIN track_dimension td ON lf.track_key = td.track_key
                    JOIN release_dimension rd ON lf.release_key = rd.release_key
                    GROUP BY td.track_name, rd.cover_image
                    ORDER BY days_since DESC
                    LIMIT 10;
                """
            }
        ]

        # Execute the queries and store results
        for query_info in data_queries:
            db_cursor.execute(query_info['sql_query'])
            query_info['results'] = db_cursor.fetchall()

        # Helper to format query results to HTML
        def format_section_html(title, results):
            html_section = f"<h3>{safe_html(title)}</h3><ul>"
            for item in results:
                if title == "Tracks not heard recently":
                    track_name, cover_image, last_play, days_since = item
                    html_section += f"<li>'{safe_html(track_name)}', last played {safe_html(str(days_since))} days ago</li>"
                else:
                    name = item[0]
                    minutes = item[-1]
                    html_section += f"<li>'{safe_html(name)}': {safe_html(str(minutes))} minutes listened</li>"
            html_section += "</ul>"
            return html_section

        # Build email HTML body with query results only
        email_html_parts = [
            format_section_html("Top 10 Tracks", data_queries[0]['results']),
            format_section_html("Top 5 Releases", data_queries[1]['results']),
            format_section_html("Top 5 Performers", data_queries[2]['results']),
            format_section_html("Tracks not heard recently", data_queries[3]['results']),
        ]

        email_body_html = "\n".join(email_html_parts)

        # Send email with the query results only
        send_notification_email(email_body_html, "Your Weekly Music Listening Data Summary 🎵")

        db_cursor.close()
        connection.close()

        return "Weekly data summary email sent successfully."

    except Exception as e:
        logging.error(f"Error in generate_weekly_insights: {e}")
        return f"Error: {str(e)}"



def safe_html(text):
    if not text:
        return ""
    return html.escape(text).replace('\n', '<br>')


def safe_str_html(value):
    if not value:
        return ""
    if not isinstance(value, str):
        value = str(value)
    return safe_html(value)


def format_dict_as_html(data: dict) -> str:
    html_parts = []
    for key, value in data.items():
        html_parts.append(f"<p><b>{safe_str_html(key)}:</b> {safe_str_html(value)}</p>")
    return "\n".join(html_parts)


def format_list_as_html(data: list) -> str:
    html_parts = "<ul>"
    for item in data:
        html_parts += f"<li>{safe_str_html(item)}</li>"
    html_parts += "</ul>"
    return html_parts

def generate_daily_insights():
    """Generate daily listening insights and send email with only raw query results"""
    try:
        connection = create_database_connection()
        db_cursor = connection.cursor()

        data_queries = [
            {
                'section_name': 'Top 10 Tracks',
                'sql_query': """
                    SELECT td.track_name, rd.release_name, pd.performer_name, rd.cover_image,
                           FLOOR(SUM(td.length_milliseconds) / (1000 * 60)) AS minutes_total
                    FROM listening_fact lf
                    JOIN track_dimension td ON lf.track_key = td.track_key
                    JOIN release_dimension rd ON lf.release_key = rd.release_key
                    JOIN performer_dimension pd ON lf.performer_key = pd.performer_key
                    WHERE lf.timestamp_played >= CURRENT_DATE
                      AND lf.timestamp_played < CURRENT_DATE + INTERVAL '1 day'
                    GROUP BY td.track_name, rd.release_name, pd.performer_name, rd.cover_image
                    ORDER BY minutes_total DESC
                    LIMIT 10;
                """
            },
            {
                'section_name': 'Top 5 Releases',
                'sql_query': """
                    SELECT rd.release_name, pd.performer_name, rd.cover_image,
                           FLOOR(SUM(td.length_milliseconds) / (1000 * 60)) AS minutes_total
                    FROM listening_fact lf
                    JOIN release_dimension rd ON lf.release_key = rd.release_key
                    JOIN performer_dimension pd ON lf.performer_key = pd.performer_key
                    JOIN track_dimension td ON lf.track_key = td.track_key
                    WHERE lf.timestamp_played >= CURRENT_DATE
                      AND lf.timestamp_played < CURRENT_DATE + INTERVAL '1 day'
                    GROUP BY rd.release_name, pd.performer_name, rd.cover_image
                    ORDER BY minutes_total DESC
                    LIMIT 5;
                """
            },
            {
                'section_name': 'Top 5 Performers',
                'sql_query': """
                    SELECT pd.performer_name,
                           FLOOR(SUM(td.length_milliseconds) / (1000 * 60)) AS minutes_total
                    FROM listening_fact lf
                    JOIN performer_dimension pd ON lf.performer_key = pd.performer_key
                    JOIN track_dimension td ON lf.track_key = td.track_key
                    WHERE lf.timestamp_played >= CURRENT_DATE
                      AND lf.timestamp_played < CURRENT_DATE + INTERVAL '1 day'
                    GROUP BY pd.performer_name
                    ORDER BY minutes_total DESC
                    LIMIT 5;
                """
            },
            {
                'section_name': 'Tracks not heard recently',
                'sql_query': """
                    SELECT td.track_name, rd.cover_image,
                           MAX(lf.timestamp_played) AS last_play,
                           (CURRENT_DATE - MAX(lf.timestamp_played)::date) AS days_since
                    FROM listening_fact lf
                    JOIN track_dimension td ON lf.track_key = td.track_key
                    JOIN release_dimension rd ON lf.release_key = rd.release_key
                    GROUP BY td.track_name, rd.cover_image
                    ORDER BY days_since DESC
                    LIMIT 10;
                """
            }
        ]

        # Execute queries and fetch results
        for query_info in data_queries:
            db_cursor.execute(query_info['sql_query'])
            query_info['results'] = db_cursor.fetchall()

        # Helper to format query results to HTML
        def format_section_html(title, results):
            html_section = f"<h3>{safe_html(title)}</h3><ul>"
            for item in results:
                if title == "Tracks not heard recently":
                    track_name, cover_image, last_play, days_since = item
                    html_section += f"<li>'{safe_html(track_name)}', last played {safe_html(str(days_since))} days ago</li>"
                else:
                    name = item[0]
                    minutes = item[-1]
                    html_section += f"<li>'{safe_html(name)}': {safe_html(str(minutes))} minutes listened</li>"
            html_section += "</ul>"
            return html_section

        # Build email HTML with all sections including new Genres section
        email_html_parts = [
            format_section_html("Top 10 Tracks", data_queries[0]['results']),
            format_section_html("Top 5 Releases", data_queries[1]['results']),
            format_section_html("Top 5 Performers", data_queries[2]['results']),
            format_section_html("Tracks not heard recently", data_queries[3]['results']) 
        ]

        email_body_html = "\n".join(email_html_parts)

        # Send email with daily raw data only
        send_notification_email(email_body_html, "Your Daily Music Listening Data Summary 🎵")

        db_cursor.close()
        connection.close()

        return "Daily data summary email sent successfully."

    except Exception as e:
        logging.error(f"Error in generate_daily_insights: {e}")
        return f"Error: {str(e)}"

def send_notification_email(html_message, subject):
    """Send email notification with customizable subject."""
    sender_address = "sanjitsanjitss30@gmail.com"
    recipient_address = "sahusanjit2001@gmail.com"
    app_password = os.getenv("MAIL_APP_KEY")

    if not app_password:
        print("Email password not configured")
        return

    email_msg = MIMEMultipart('alternative')
    email_msg['Subject'] = subject
    email_msg['From'] = sender_address
    email_msg['To'] = recipient_address

    html_section = MIMEText(html_message, 'html')
    email_msg.attach(html_section)

    try:
        with smtplib.SMTP('smtp.gmail.com', 587) as mail_server:
            mail_server.starttls()
            mail_server.login(sender_address, app_password)
            mail_server.sendmail(sender_address, recipient_address, email_msg.as_string())
            print("Email sent successfully!")
    except Exception as e:
        print(f"Error sending email: {e}")

