from confluent_kafka import Consumer, KafkaException
import psycopg2
import json
import os
from dotenv import load_dotenv
from datetime import datetime
import time
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
    'group.id': 'listening-events-consumer',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': True,
    'auto.commit.interval.ms': 1000
}

def handle_listening_message(message_data, db_cursor, db_connection):
    """Process listening message and insert into database"""
    try:
        # Process timestamp_played
        timestamp_value = message_data['timestamp_played']
        if isinstance(timestamp_value, list):
            timestamp_value = timestamp_value[0]
        
        # Skip invalid timestamps
        if not timestamp_value or len(timestamp_value) < 10:  # Basic validation
            logger.warning(f"Invalid timestamp: {timestamp_value}")
            return False
        
        # Handle timezone
        if timestamp_value.endswith('Z'):
            timestamp_value = timestamp_value.replace('Z', '+00:00')
        
        try:
            timestamp_dt = datetime.fromisoformat(timestamp_value)
        except ValueError as e:
            logger.warning(f"Invalid datetime format: {timestamp_value}, error: {e}")
            return False
        
        # Wait for dimension tables to be populated first
        time.sleep(2)
        
        db_cursor.execute(
            """
            INSERT INTO listening_fact (track_key, release_key, performer_key, timestamp_played)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT DO NOTHING;
            """,
            (
                message_data["track_key"],
                message_data["release_key"],
                message_data["performer_key"],
                timestamp_dt
            )
        )
        db_connection.commit()
        logger.info(f"Listening record processed: {message_data['track_key']} played at {timestamp_dt}")
        return True
    
    except Exception as e:
        logger.error(f"Error processing listening message: {e}")
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
        message_consumer.subscribe(['listening_events_topic'])
        logger.info("Subscribed to listening_events_topic")
        
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
                    logger.info("[SCHEMA] Fields in listening message:")
                    for field_key, field_value in message_data.items():
                        logger.info(f"  {field_key}: {type(field_value).__name__}")
                    schema_displayed = True
                
                # Process message
                logger.info(f"[DATA] Processing listening event for track: {message_data.get('track_key', 'Unknown')}")
                processing_success = handle_listening_message(message_data, db_cursor, db_connection)
                
                if processing_success:
                    logger.info("[INFO] Listening record written successfully")
                else:
                    logger.warning("[WARNING] Failed to process listening record")
                
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
    logger.info("Starting listening subscriber...")
    main()