import os
import datetime
from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker
from .models import GoogleToken, GmailHistory, PressReleaseSummary, EmailsTriggered
import logging
import json

logger = logging.getLogger(__name__)

class DBService:
    """Database connector and session manager for Google token and Gmail history tracking."""
    
    def __init__(self):
        """Initialize DB connection using environment variables."""
        self.engine = create_engine(
            f"postgresql+psycopg2://"
            f"{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@"
            f"{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/"
            f"{os.getenv('DB_NAME')}"
        )
        self.Session = scoped_session(sessionmaker(bind=self.engine))
    
    def get_session(self):
        """Get a new SQLAlchemy session."""
        return self.Session()
    
    def close(self):
        """Dispose engine and remove session bindings."""
        self.Session.remove()
        self.engine.dispose()

    def get_google_token(self, email):
        """
        Fetch stored Google token by email.

        :param email: Email address of the user
        :return: GoogleToken instance or None
        """
        session = self.get_session()
        try:
            logger.debug(f"Fetching Google token for {email}")
            return session.query(GoogleToken).filter_by(email=email).first()
        finally:
            session.close()
    
    def save_google_token(self, email, creds):
        """
        Save or update Google OAuth2 credentials in the database.

        :param email: Email address of the user
        :param creds: OAuth2 Credentials object
        :return: GoogleToken instance
        """
        session = self.get_session()
        try:
            token_record = session.query(GoogleToken).filter_by(email=email).first()
            expires_at = datetime.datetime.now() + datetime.timedelta(
                seconds=creds.expiry.timestamp() - datetime.datetime.now().timestamp()
            )

            if token_record:
                logger.debug(f"Updating existing Google token for {email}")
                token_record.access_token = creds.token
                token_record.refresh_token = creds.refresh_token 
                token_record.token_uri = creds.token_uri
                token_record.client_id = creds.client_id
                token_record.client_secret = creds.client_secret
                token_record.scopes = ','.join(creds.scopes)
                token_record.expires_at = expires_at
                token_record.updated_at = datetime.datetime.now()
            else:
                logger.debug(f"Creating new Google token for {email}")
                token_record = GoogleToken(
                    email=email,
                    access_token=creds.token,
                    refresh_token=creds.refresh_token,
                    token_uri=creds.token_uri,
                    client_id=creds.client_id,
                    client_secret=creds.client_secret,
                    scopes=','.join(creds.scopes),
                    expires_at=expires_at
                )
                session.add(token_record)
            
            session.commit()
            logger.info(f"Saved credentials to database for {email}")
            return token_record

        except Exception as e:
            session.rollback()
            logger.error(f"Error saving credentials to database for {email}: {e}")
            raise
        finally:
            session.close()

    def get_last_history_id(self, user_email: str):
        """
        Fetch the last saved Gmail historyId for a user.

        :param user_email: Gmail address of the user
        :return: History ID as a string if found, else None
        """
        session = self.get_session()
        record = session.query(GmailHistory).filter_by(user_email=user_email).first()
        session.close()

        if record:
            logger.info(f"Found historyId {record.history_id} for user {user_email}")
            return str(record.history_id)
        else:
            logger.info(f"No historyId found for user {user_email}")
            return None

    def save_last_history_id(self, user_email: str, history_id: str):
        """
        Store or update the Gmail historyId for a user.

        :param user_email: Gmail address of the user
        :param history_id: Latest Gmail history ID to save
        """
        session = self.get_session()
        record = session.query(GmailHistory).filter_by(user_email=user_email).first()

        if record:
            record.history_id = int(history_id)
            logger.info(f"Updated historyId for user {user_email} to {history_id}")
        else:
            session.add(GmailHistory(user_email=user_email, history_id=int(history_id)))
            logger.info(f"Created historyId record for user {user_email} with {history_id}")

        session.commit()
        session.close()

    def publish_summary_metric(self, summary_metrics):
        """
        Publish a single summary metric to PostgreSQL.
        """
        session = self.get_session()

        try:
            press_release_summary = PressReleaseSummary(
                message_id=summary_metrics.get('msg_id'),
                release_timestamp=summary_metrics.get('release_timestamp'),
                email_delivery_time=summary_metrics.get('email_delivery_time'),
                retrieved_timestamp=summary_metrics.get('retrieved_timestamp'),
                summary_ts=summary_metrics.get('summary_ts'),
                email_sender=summary_metrics.get('email_sender'),
                email_subject=summary_metrics.get('email_subject'),
                email_body=summary_metrics.get('email_body'),
                link_to_news_release_from_email=summary_metrics.get('link_to_news_release_from_email'),
                link_selection_method_from_email=summary_metrics.get('link_selection_method_from_email'),
                all_available_links_from_email=json.dumps(summary_metrics.get('all_available_links_from_email')),
                main_content_from_news_release_page=summary_metrics.get('main_content_from_news_release_page'),
                pdf_count=summary_metrics.get('pdf_count'),
                analyzed_pdf_count=summary_metrics.get('analyzed_pdf_count'),
                page_summary=summary_metrics.get('page_summary'),
                email_summary=summary_metrics.get('email_summary')
            )
            session.add(press_release_summary)
            session.commit()
            logger.info(f"Successfully added the summary metric for {summary_metrics.get('msg_id')} to the database.")
        except Exception as e:
            session.rollback()
            logger.error(f"Error adding summary metric to the database: {e}")
            raise
        finally:
            session.close()

    def set_email_triggered(self, message_id):
        """
        Mark an email as triggered in the database.

        :param message_id: Unique identifier for the email
        """
        session = self.get_session()
        try:
            email_triggered = EmailsTriggered(message_id=message_id)
            session.add(email_triggered)
            session.commit()
            logger.info(f"Email with message ID {message_id} marked as triggered.")
        except Exception as e:
            session.rollback()
            logger.error(f"Error marking email as triggered: {e}")
            raise
        finally:
            session.close()