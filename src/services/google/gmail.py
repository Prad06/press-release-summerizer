import os
import logging
import json
from googleapiclient.discovery import build
from .auth import get_google_credentials

logger = logging.getLogger(__name__)

class GmailService:
    """Service for interacting with the Gmail API"""

    SCOPES = ['https://www.googleapis.com/auth/gmail.readonly']

    def __init__(self, db_service):
        """
        Initialize the GmailService with credentials for a specific user.

        :param db_service: Instance of DBService (unused for now)
        """
        self.db_service = db_service
        self.user_email = os.getenv('GMAIL_USER_EMAIL')
        self.service = None
        logger.info(f"GmailService initialized for user: {self.user_email}")

    def get_service(self):
        """Authenticate and return the Gmail API client."""
        if self.service is None:
            logger.info("Gmail service not initialized, starting authentication...")
            creds = get_google_credentials(
                self.db_service,
                self.user_email,
                self.SCOPES
            )
            if not creds:
                logger.error("Failed to obtain credentials.")
                return None

            try:
                self.service = build('gmail', 'v1', credentials=creds)
                logger.info("Gmail service created successfully.")
            except Exception as e:
                logger.error(f"Failed to create Gmail service: {e}")
                return None
        return self.service

    def list_messages(self, query=""):
        """List Gmail messages matching a query."""
        service = self.get_service()
        if not service:
            return None

        try:
            results = service.users().messages().list(userId='me', q=query).execute()
            return results.get('messages', [])
        except Exception as e:
            logger.error(f"Error listing messages: {e}")
            return None

    def get_message(self, message_id):
        """Get a full Gmail message by its ID."""
        service = self.get_service()
        if not service:
            return None

        try:
            return service.users().messages().get(userId='me', id=message_id).execute()
        except Exception as e:
            logger.error(f"Error getting message {message_id}: {e}")
            return None

    def extract_headers(self, message):
        """Extract headers from a Gmail message object."""
        if not message or 'payload' not in message or 'headers' not in message['payload']:
            return {}
        return {header['name']: header['value'] for header in message['payload']['headers']}

    def fetch_new_emails(self, history_id):
        """
        Fetch all new message IDs added since the provided Gmail history ID, handling pagination.

        :param history_id: The starting Gmail history ID (from Pub/Sub)
        :return: A tuple (list of message IDs, latest history ID seen)
        """
        service = self.get_service()
        if not service:
            return [], history_id

        message_ids = []
        latest_history_id = history_id
        page_token = None

        while True:
            try:
                response = service.users().history().list(
                    userId='me',
                    startHistoryId=history_id,
                    historyTypes='messageAdded',
                    pageToken=page_token,
                    maxResults=100  # Gmail allows up to 100
                ).execute()

                # Append message IDs
                for record in response.get('history', []):
                    for msg in record.get('messages', []):
                        message_ids.append(msg['id'])

                latest_history_id = response.get('historyId', latest_history_id)
                page_token = response.get('nextPageToken')

                if not page_token:
                    break
            except Exception as e:
                logger.error(f"Error during Gmail history pagination: {e}")
                break

        logger.info(f"Fetched {len(message_ids)} new message(s) since history ID {history_id}")
        return message_ids, latest_history_id

    def get_message_details(self, message_id):
        """
        Fetch metadata from a Gmail message by ID.

        :param message_id: Gmail message ID
        :return: Dictionary with message metadata
        """
        message = self.get_message(message_id)
        if not message:
            return None

        headers = self.extract_headers(message)
        return {
            'message_id': message_id,
            'from': headers.get('From', 'Unknown sender'),
            'to': headers.get('To', 'Unknown recipient'),
            'subject': headers.get('Subject', 'No subject'),
            'date': headers.get('Date', 'Unknown date'),
            'message': message
        }

    def process_notification(self, data, target_email=None):
        """
        Process a Gmail notification triggered via Pub/Sub.

        :param data: Parsed Pub/Sub data containing 'historyId'
        :param target_email: Optional email to match against the 'To' field
        :return: Tuple (success, message_details) or (False, None) on error
        """
        try:
            if isinstance(data, str):
                data = json.loads(data)

            history_id = str(data.get('historyId'))
            if not history_id:
                logger.warning("Missing 'historyId' in Gmail notification.")
                return False, None

            logger.info(f"Processing Gmail notification with history ID: {history_id}")

            # Retrieve the last saved history ID from the database
            last_history_id = self.db_service.get_last_history_id(self.user_email)
            if not last_history_id:
                logger.warning("No previous history ID found. Skipping processing and saving this one.")
                self.db_service.save_last_history_id(self.user_email, history_id)
                return True, None

            # Fetch all new message IDs since the last known history ID
            new_message_ids, new_history_id = self.fetch_new_emails(last_history_id)

            if not new_message_ids:
                logger.info("No new messages found in this history range.")
                self.db_service.save_last_history_id(self.user_email, new_history_id)
                return True, None
            
            matching_messages = []
            for msg_id in new_message_ids:
                message_details = self.get_message_details(msg_id)
                if not message_details:
                    continue

                message_details["matches_target"] = False
                if target_email and target_email.lower() in message_details.get("to", "").lower():
                    message_details["matches_target"] = True
                    logger.info(f"Message {msg_id} matches target email: {target_email}")
                    matching_messages.append(message_details)

            # Save latest history ID even if no messages matched
            self.db_service.save_last_history_id(self.user_email, new_history_id)

            if matching_messages:
                return True, matching_messages
            else:
                logger.info("No matching messages found.")
                return True, None

        except Exception as e:
            logger.error(f"Error processing Gmail notification: {e}")
            return False, None
