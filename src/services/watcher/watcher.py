import logging
import os

from src.services.db import DBService
from src.services.google import GmailService, PubSubService

logger = logging.getLogger(__name__)

class WatcherService:
    def __init__(self):
        self.db_service = DBService()
        self.gmail_service = GmailService(self.db_service)
        self.pubsub_service = PubSubService(self.db_service)
        self.target_email = os.getenv("TARGET_EMAIL")

    def start(self):
        logger.info("Starting WatcherService...")

        if not self.pubsub_service.setup_gmail_watch(self.gmail_service):
            logger.error("Failed to initialize Gmail watch.")
            return

        def handler(message):
            return self.process_gmail_message(message)

        self.pubsub_service.listen_for_messages(handler)

    def process_gmail_message(self, message):
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

        for i in range(0, len(matched), 50):
            batch = matched[i:i + 50]
            message_ids = [m['message_id'] for m in batch]
            logger.info(f"Triggering Airflow for batch: {message_ids}")
            # trigger_airflow_dag_batch(message_ids)

        return True

    def stop(self):
        logger.info("Stopping WatcherService.")
        self.db_service.close()
