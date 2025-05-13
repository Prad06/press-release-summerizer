import logging
import os

from src.services.db import DBService
from src.services.google import GmailService, PubSubService

from dotenv import load_dotenv
load_dotenv()

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration
TARGET_EMAIL = os.getenv('TARGET_EMAIL')

def process_gmail_message(message, gmail_service):
    """
    Process Gmail notification message from PubSub
    
    :param message: PubSub message
    :param gmail_service: GmailService instance
    :return: True if processing successful, False otherwise
    """
    try:
        # Get pubsub service for parsing
        db_service = DBService()
        try:
            pubsub_service = PubSubService(db_service)
            
            # Parse the message
            data = pubsub_service.parse_pubsub_message(message)
            
            if not data:
                return False
            
            # Process the message with Gmail service
            success, message_details = gmail_service.process_notification(data, TARGET_EMAIL)

            if success:
                if message_details:
                    if not isinstance(message_details, list):
                        message_details = [message_details]

                    logger.info(f"Processing {len(message_details)} new message(s)")

                    # Filter messages that match target email
                    matched = [msg for msg in message_details if msg.get("matches_target")]

                    if matched:
                        logger.info(f"Found {len(matched)} message(s) matching the target address")

                        # Batch and trigger Airflow
                        batch_size = 50
                        for i in range(0, len(matched), batch_size):
                            batch = matched[i:i + batch_size]
                            message_ids = [msg["message_id"] for msg in batch]
                            logger.info(f"Triggering Airflow DAG for batch of {len(batch)} messages")
                            logger.info(f"Message IDs: {message_ids}")

                            # Trigger your DAG here (pseudo code)
                            # trigger_airflow_dag_batch(message_ids)

                    else:
                        logger.info("No matching messages found.")

                    # Log details for all messages
                    for message in message_details:
                        logger.info(f"\n📬 Email Details:")
                        logger.info(f"   From: {message['from']}")
                        logger.info(f"   To: {message['to']}")
                        logger.info(f"   Subject: {message['subject']}")
                        logger.info(f"   Date: {message['date']}")
                        logger.info(f"   Message ID: {message['message_id']}")

                    return True
                else:
                    logger.info("No new messages found in this history range.")
                    return True
            else:
                return False
        finally:
            db_service.close()
            
    except Exception as e:
        logger.error(f"Error in process_gmail_message: {e}")
        return False

def main():
    """Main entry point"""
    # Ensure environment variables are set
    if not os.getenv('GOOGLE_CLOUD_PROJECT'):
        print("ERROR: GOOGLE_CLOUD_PROJECT environment variable not set")
        return 1
        
    logger.info("Starting Gmail PubSub Monitor")
    
    # Initialize services
    db_service = DBService()
    try:
        gmail_service = GmailService(db_service)
        pubsub_service = PubSubService(db_service)
        
        # Set up the Gmail watch
        if not pubsub_service.setup_gmail_watch(gmail_service):
            logger.error("Failed to set up Gmail watch")
            return 1
        
        # Define a message handler that uses our gmail_service
        def message_handler(message):
            return process_gmail_message(message, gmail_service)
        
        # Start listening for messages
        pubsub_service.listen_for_messages(message_handler)
        
    except Exception as e:
        logger.error(f"Error in main process: {e}")
        return 1
    finally:
        db_service.close()
        
    return 0

if __name__ == "__main__":
    exit(main())