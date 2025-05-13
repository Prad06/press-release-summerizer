import logging
import os

from src.services.db import DBService
from src.services.google import GmailService, PubSubService
from src.services.trigger import AirflowTrigger

logger = logging.getLogger(__name__)

class WatcherService:
    def __init__(self):
        """
        A service that watches for Gmail notifications, processes them, and triggers downstream workflows.

        Attributes:
            db_service (DBService): Service for database interactions.
            gmail_service (GmailService): Service for interacting with Gmail API.
            pubsub_service (PubSubService): Service for interacting with Pub/Sub.
            airflow_trigger (AirflowTrigger): Service to trigger Airflow DAGs.
            target_email (str): The target email address to monitor.
            dag_id (str): The DAG ID to trigger.
        """
        self.db_service = DBService()
        self.gmail_service = GmailService(self.db_service)
        self.pubsub_service = PubSubService(self.db_service)
        self.airflow_trigger = AirflowTrigger()
        self.target_email = os.getenv("TARGET_EMAIL")
        self.dag_id = os.getenv("PROCESS_GMAIL_DAG_ID")

        if not self.dag_id:
            raise ValueError("PROCESS_GMAIL_DAG_ID environment variable not set.")

    def start(self):
        """
        Starts the WatcherService by setting up Gmail watch and listening for Pub/Sub messages.
        """
        logger.info("Starting WatcherService...")

        if not self.pubsub_service.setup_gmail_watch(self.gmail_service):
            logger.error("Failed to initialize Gmail watch.")
            return

        self.pubsub_service.listen_for_messages(self.process_gmail_message)

    def process_gmail_message(self, message):
        """
        Processes a Gmail notification message received from Pub/Sub.

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

        matched = [msg for msg in message_details if msg.get("matches_target")]
        logger.info(f"Found {len(matched)} message(s) matching target.")

        for i in range(0, len(matched), 50):
            batch = matched[i:i + 50]
            message_ids = [m['message_id'] for m in batch]
            logger.info(f"Triggering Airflow for batch: {message_ids}")
            status = self.airflow_trigger.trigger_dag(self.dag_id, email_address=self.target_email, message_ids=message_ids)

            if status.get("status") == "error":
                logger.error(f"Error triggering DAG: {status.get('error')}")
                return False

        logger.info(f"Processed {len(matched)} matching message(s).")
        return True

    def stop(self):
        """
        Stops the WatcherService by closing the database connection.
        """
        logger.info("Stopping WatcherService.")
        self.db_service.close()
