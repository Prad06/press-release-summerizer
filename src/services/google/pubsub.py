import os
import time
import logging
from google.cloud import pubsub_v1
from google.oauth2 import service_account

logger = logging.getLogger(__name__)

class PubSubService:
    """Service for interacting with Google Pub/Sub"""
    
    def __init__(self, db_service):
        """
        Initialize PubSub service
        
        :param db_service: DBService for database access
        """
        self.db_service = db_service
        self.publisher = None
        self.subscriber = None
        self.project_id = os.getenv('GOOGLE_CLOUD_PROJECT')
        self.topic_name = os.getenv('PUBSUB_TOPIC_NAME')
        self.subscription_name = os.getenv('PUBSUB_SUBSCRIPTION_NAME')
        self.scopes = ['https://www.googleapis.com/auth/pubsub', 
                        'https://www.googleapis.com/auth/cloud-platform',
                        'https://www.googleapis.com/auth/gmail.modify']
        
        # Path to service account credentials
        self.service_account_path = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
        
        logger.info(f"PubSubService initialized for project: {self.project_id}")
        logger.info(f"Using topic: {self.topic_name}, subscription: {self.subscription_name}")
    
    def get_publisher(self):
        """
        Get or create publisher client using service account
        
        :return: Publisher client or None if authentication fails
        """
        if self.publisher is None:
            try:
                # Use service account credentials directly
                credentials = service_account.Credentials.from_service_account_file(
                    self.service_account_path,
                    scopes=self.scopes
                )
                
                self.publisher = pubsub_v1.PublisherClient(credentials=credentials)
                logger.info("PubSub publisher created successfully using service account")
            except Exception as e:
                logger.error(f"Failed to create PubSub publisher: {e}")
                return None
                
        return self.publisher
        
    def get_subscriber(self):
        """
        Get or create subscriber client using service account
        
        :return: Subscriber client or None if authentication fails
        """
        if self.subscriber is None:
            try:
                # Use service account credentials directly
                credentials = service_account.Credentials.from_service_account_file(
                    self.service_account_path,
                    scopes=self.scopes
                )
                
                self.subscriber = pubsub_v1.SubscriberClient(credentials=credentials)
                logger.info("PubSub subscriber created successfully using service account")
            except Exception as e:
                logger.error(f"Failed to create PubSub subscriber: {e}")
                return None
                
        return self.subscriber
    
    def setup_gmail_watch(self, gmail_service, topic_name=None):
        """
        Set up Gmail push notification for new messages
        
        :param gmail_service: Initialized GmailService instance
        :param topic_name: Topic name (default: use self.topic_name)
        :return: Watch response or None if setup fails
        """
        # Get the Gmail service
        service = gmail_service.get_service()
        if not service:
            logger.error("Failed to get Gmail service")
            return None
        
        # Use the provided topic name or the default one
        target_topic = topic_name or self.topic_name
        if not target_topic:
            logger.error("No topic name provided")
            return None
        
        # Ensure the topic name is fully qualified
        if not target_topic.startswith('projects/'):
            topic_path = f"projects/{self.project_id}/topics/{target_topic}"
        else:
            topic_path = target_topic
        
        try:
            # Set up watch request
            request = {
                'labelIds': ['INBOX'],
                'topicName': topic_path
            }
            
            # Execute the watch request
            response = service.users().watch(userId='me', body=request).execute()
            
            # Calculate expiration time
            expiration_ms = int(response.get('expiration', '0'))
            expiration_seconds = expiration_ms / 1000
            expiration_days = (expiration_seconds - time.time()) / (60 * 60 * 24)
            
            logger.info(f"Gmail watch set up successfully for topic: {topic_path}")
            logger.info(f"Watch will expire in approximately {expiration_days:.1f} days")
            
            history_id = response.get("historyId")
            if history_id:
                self.db_service.save_last_history_id(
                    gmail_service.user_email,
                    str(history_id)
                )
                logger.info(f"Stored initial historyId: {history_id}")
            else:
                logger.warning("Watch response did not contain a historyId.")

            return response
        except Exception as e:
            logger.error(f"Error setting up Gmail watch: {e}")
            return None
    
    def listen_for_messages(self, handler_func, subscription_name=None):
        """
        Listen for messages on a subscription
        
        :param handler_func: Function to handle received messages, gets message as parameter
        :param subscription_name: Subscription name (default: use self.subscription_name)
        """
        target_subscription = subscription_name or self.subscription_name
        
        try:
            subscriber = self.get_subscriber()
            if not subscriber:
                logger.error("Failed to get subscriber client")
                return
                
            subscription_path = subscriber.subscription_path(self.project_id, target_subscription)
            
            def callback(message):
                try:
                    # Log basic info
                    logger.info(f"Received message: ID={message.message_id}")
                    
                    # Call the handler function
                    success = handler_func(message)
                    
                    # Acknowledge or not based on success
                    if success:
                        message.ack()
                        logger.info(f"Message {message.message_id} processed successfully")
                    else:
                        message.nack()
                        logger.warning(f"Failed to process message {message.message_id}. Will retry.")
                except Exception as e:
                    logger.error(f"Error in message callback: {e}")
                    message.nack()
            
            # Set up streaming pull
            streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
            logger.info(f"Listening for messages on {subscription_path}...")
            logger.info(f"[Waiting for new messages... Press Ctrl+C to exit]")
            
            # Keep the main thread from exiting
            try:
                # Block indefinitely
                streaming_pull_future.result()
            except KeyboardInterrupt:
                logger.info("\nStopping listener due to keyboard interrupt")
                streaming_pull_future.cancel()
            except Exception as e:
                logger.error(f"Exception during message listening: {e}")
                streaming_pull_future.cancel()
                
        except Exception as e:
            logger.error(f"Error in listen_for_messages: {e}")
    
    def parse_pubsub_message(self, message):
        """
        Parse a PubSub message into a Python object
        
        :param message: PubSub message object
        :return: Dictionary with parsed data or None if parsing fails
        """
        try:
            raw = message.data.decode("utf-8", errors="replace")
            logger.info(f"Raw message payload: {raw}")
            return raw
        except Exception as e:
            logger.error(f"Failed to decode PubSub message: {e}")
            return None