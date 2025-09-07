from confluent_kafka import Consumer, KafkaException
import psycopg2
import json
import os
from dotenv import load_dotenv
from datetime import datetime
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# Database connection
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
        logger.error(f"Database connection error: {e}")
        raise

# Message consumer configuration
consumer_settings = {
    'bootstrap.servers': 'message_broker:29092',
    'group.id': 'release-data-consumer',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': True,
    'auto.commit.interval.ms': 1000
}

def handle_release_message(message_data, db_cursor, db_connection):
    """Process release message and insert into database"""
    try:
        # Convert publish_date string to datetime if needed
        publish_date_value = message_data.get('publish_date', '')
        if publish_date_value:
            # Handle different date formats from Spotify
            try:
                if len(publish_date_value) == 4:  # Only year
                    publish_date_value = f"{publish_date_value}-01-01"
                elif len(publish_date_value) == 7:  # Year-month
                    publish_date_value = f"{publish_date_value}-01"
                
                publish_date_value = datetime.strptime(publish_date_value, '%Y-%m-%d').date()
            except ValueError:
                logger.warning(f"Invalid date format: {publish_date_value}, setting to None")
                publish_date_value = None
        
        db_cursor.execute(
            """
            INSERT INTO release_dimension (release_key, release_name, track_count, publish_date,
                                          external_link, cover_image, publisher_name, popularity_index)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (release_key) DO NOTHING;
            """,
            (
                message_data["release_key"],
                message_data["release_name"],
                message_data["track_count"],
                publish_date_value,
                message_data["external_link"],
                message_data["cover_image"],
                message_data.get("publisher_name", "Unknown"),
                message_data["popularity_index"]
            )
        )
        db_connection.commit()
        logger.info(f"Release record processed: {message_data['release_name']}")
        return True
    
    except Exception as e:
        logger.error(f"Error processing release message: {e}")
        db_connection.rollback()
        return False

def main():
    """Main consumer loop"""
    db_connection = None
    db_cursor = None
    message_consumer = None
    
    try:
        # Initialize database connection
        db_connection = create_database_connection()
        db_cursor = db_connection.cursor()
        logger.info("Database connection established")
        
        # Initialize message consumer
        message_consumer = Consumer(consumer_settings)
        message_consumer.subscribe(['release_data_topic'])
        logger.info("Subscribed to release_data_topic")
        
        schema_displayed = False
        
        while True:
            message = message_consumer.poll(timeout=1.0)
            
            if message is None:
                continue
            
            if message.error():
                if message.error().code() == KafkaException._PARTITION_EOF:
                    logger.info(f"Reached end of partition {message.partition()}")
                else:
                    logger.error(f"Consumer error: {message.error()}")
                continue
            
            try:
                # Parse message
                message_data = json.loads(message.value().decode('utf-8'))
                
                # Print schema on first record
                if not schema_displayed:
                    logger.info("[SCHEMA] Fields in release message:")
                    for field_key, field_value in message_data.items():
                        logger.info(f"  {field_key}: {type(field_value).__name__}")
                    schema_displayed = True
                
                # Process message
                logger.info(f"[DATA] Processing release: {message_data.get('release_name', 'Unknown')}")
                processing_success = handle_release_message(message_data, db_cursor, db_connection)
                
                if processing_success:
                    logger.info("[INFO] Release record written successfully")
                else:
                    logger.warning("[WARNING] Failed to process release record")
                
            except json.JSONDecodeError as e:
                logger.error(f"Failed to decode JSON message: {e}")
            except Exception as e:
                logger.error(f"Error processing message: {e}")
    
    except KeyboardInterrupt:
        logger.info("Consumer stopped by user")
    except Exception as e:
        logger.error(f"Fatal error in consumer: {e}")
    finally:
        # Clean up resources
        if message_consumer:
            message_consumer.close()
            logger.info("Message consumer closed")
        if db_cursor:
            db_cursor.close()
        if db_connection:
            db_connection.close()
            logger.info("Database connection closed")

if __name__ == "__main__":
    logger.info("Starting release subscriber...")
    main()