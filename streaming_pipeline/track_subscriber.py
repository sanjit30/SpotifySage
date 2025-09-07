from confluent_kafka import Consumer, KafkaException
import psycopg2
import json
import os
from dotenv import load_dotenv
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
    'group.id': 'track-data-consumer',  
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': True,
    'auto.commit.interval.ms': 1000
}

def handle_track_message(message_data, db_cursor, db_connection):
    """Process track message and insert into database"""
    try:
        db_cursor.execute(
            """
            INSERT INTO track_dimension (track_key, track_name, disc_position, length_milliseconds, contains_explicit,
                                        external_link, sample_url, popularity_index)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (track_key) DO NOTHING;
            """,
            (
                message_data["track_key"],
                message_data["track_name"],
                message_data["disc_position"],
                message_data["length_milliseconds"],
                message_data["contains_explicit"],
                message_data["external_link"],
                message_data.get("sample_url", "No preview available"),
                message_data["popularity_index"]
            )
        )
        db_connection.commit()
        logger.info(f"Track record processed: {message_data['track_name']}")
        return True
    
    except Exception as e:
        logger.error(f"Error processing track message: {e}")
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
        message_consumer.subscribe(['track_data_topic'])
        logger.info("Subscribed to track_data_topic")
        
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
                    logger.info("[SCHEMA] Fields in track message:")
                    for field_key, field_value in message_data.items():
                        logger.info(f"  {field_key}: {type(field_value).__name__}")
                    schema_displayed = True
                
                # Process message
                logger.info(f"[DATA] Processing track: {message_data.get('track_name', 'Unknown')}")
                processing_success = handle_track_message(message_data, db_cursor, db_connection)
                
                if processing_success:
                    logger.info("[INFO] Track record written successfully")
                else:
                    logger.warning("[WARNING] Failed to process track record")
                
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
    logger.info("Starting track subscriber...")
    main()