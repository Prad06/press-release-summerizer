import os
import datetime
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
import logging

logger = logging.getLogger(__name__)

def get_google_credentials(db_service, email, scopes):
    """
    Get valid Google credentials for the specified email and scopes
    
    :param db_service: DBService instance for database access
    :param email: Email address identifier for the token
    :param scopes: List of OAuth scopes to request
    :return: Valid credentials object or None if authentication fails
    """
    try:
        # Try to find existing token in database
        token_record = db_service.get_google_token(email)
        creds = None
        
        if token_record:
            # Convert database record to Credentials object
            creds = Credentials(
                token=token_record.access_token,
                refresh_token=token_record.refresh_token,
                token_uri=token_record.token_uri,
                client_id=token_record.client_id,
                client_secret=token_record.client_secret,
                scopes=token_record.scopes.split(',')
            )
            
            # Check if token is expired
            now = datetime.datetime.now()
            expires_at = token_record.expires_at
            
            if now >= expires_at:
                logger.debug(f"Refreshing expired credentials for {email}")
                creds.refresh(Request())
                db_service.save_google_token(email, creds)
        
        # If no valid credentials found, initiate new OAuth flow
        if not creds:
            oauth_credentials_path = os.getenv('GMAIL_OAUTH2_CREDENTIALS')
            if not oauth_credentials_path:
                logger.error("OAuth2 credentials path not configured")
                return None
                
            logger.info(f"Initiating new OAuth2 flow for {email}")
            flow = InstalledAppFlow.from_client_secrets_file(
                oauth_credentials_path,
                scopes
            )
            creds = flow.run_local_server(port=0)
            
            # Save the new credentials to database
            db_service.save_google_token(email, creds)
        
        return creds
        
    except Exception as e:
        logger.error(f"Error getting credentials: {e}")
        return None