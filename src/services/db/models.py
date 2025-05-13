import uuid
from sqlalchemy import Column, String, Integer, BigInteger, Text, DateTime, func
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class GoogleToken(Base):
    """Model for storing Google OAuth tokens"""
    
    __tablename__ = 'google_tokens'
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    email = Column(String(120), nullable=False, unique=True)
    access_token = Column(Text, nullable=False)
    refresh_token = Column(Text, nullable=False)
    token_uri = Column(String(200), default="https://oauth2.googleapis.com/token")
    client_id = Column(String(200))
    client_secret = Column(String(200))
    scopes = Column(Text)
    expires_at = Column(DateTime, nullable=False)
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())
    
    def __repr__(self):
        return f"<GoogleToken(email='{self.email}')>"

class GmailHistory(Base):
    __tablename__ = 'gmail_history'

    id = Column(Integer, primary_key=True)
    user_email = Column(String, unique=True, nullable=False)
    history_id = Column(BigInteger, nullable=False)
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())