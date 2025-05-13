import logging
import os

from src.services.db import DBService
from src.services.google import GmailService, PubSubService
from .trigger import trigger_airflow_dag

logger = logging.getLogger(__name__)

class WatcherService:
    def __init__(self):
        """
        A service that watches for Gmail notifications, processes them, and triggers downstream workflows.

        Attributes:
            db_service (DBService): Service for database interactions.
            gmail_service (GmailService): Service for interacting with Gmail API.
            pubsub_service (PubSubService): Service for interacting with Pub/Sub.
            target_email (str): The target email address to monitor, fetched from environment variables.
        """
        self.db_service = DBService()
        self.gmail_service = GmailService(self.db_service)
        self.pubsub_service = PubSubService(self.db_service)
        self.target_email = os.getenv("TARGET_EMAIL")

    def start(self):
        """
        Starts the WatcherService by setting up Gmail watch and listening for Pub/Sub messages.

        Logs an error and stops if Gmail watch setup fails.
        """
        logger.info("Starting WatcherService...")

        if not self.pubsub_service.setup_gmail_watch(self.gmail_service):
            logger.error("Failed to initialize Gmail watch.")
            return

        def handler(message):
            return self.process_gmail_message(message)

        self.pubsub_service.listen_for_messages(handler)

    def process_gmail_message(self, message):
        """
        Processes a Gmail notification message received from Pub/Sub.

        Steps:
            - Parses the Pub/Sub message.
            - Processes the Gmail notification using GmailService.
            - Filters messages matching the target email.
            - Triggers an Airflow DAG for batches of matching messages.
        
        :param message: The Pub/Sub message containing Gmail notification.
        :return: True if processing was successful, False otherwise.
        """
        data = self.pubsub_service.parse_pubsub_message(message)
        if not data:
            return False

        success, message_details = self.gmail_service.process_notification(data, self.target_email)

        if not success:
            return False

        if not message_details:
            logger.info("No new messages found.")
            return True

        if not isinstance(message_details, list):
            message_details = [message_details]

        logger.info(f"Found {len(message_details)} message(s).")

        matched = [msg for msg in message_details if msg.get("matches_target")]

        dag_id = os.environ.get("PROCESS_GMAIL_DAG_ID")
        if not dag_id:
            logger.error("DAG ID not set in environment variables.")
            return False

        for i in range(0, len(matched), 50):
            batch = matched[i:i + 50]
            message_ids = [m['message_id'] for m in batch]
            logger.info(f"Triggering Airflow for batch: {message_ids}")
            trigger_airflow_dag(dag_id, self.target_email, message_ids)
            logger.info(f"Triggered Airflow DAG for batch of {len(batch)} messages")
        logger.info(f"Processed {len(matched)} message(s) matching the target address.")

        return True

    def stop(self):
        """
        Stops the WatcherService by closing the database connection.
        """
        logger.info("Stopping WatcherService.")
        self.db_service.close()
