import requests
import chardet
import re
import html
from concurrent.futures import ThreadPoolExecutor, as_completed
from email import message_from_bytes
from email.utils import parsedate_to_datetime, getaddresses
from urllib.parse import urlparse
from bs4 import BeautifulSoup
from typing import List, Optional, Dict
from pydantic import BaseModel, EmailStr
from datetime import datetime

class Attachment(BaseModel):
    filename: str
    content_type: str
    size: int
    content_id: Optional[str] = None

class EmailMessageModel(BaseModel):
    message_id: str
    subject: Optional[str]
    sender: Optional[EmailStr]
    to: List[EmailStr] = []
    date: Optional[datetime]
    text_body: Optional[str]
    html_body: Optional[str]
    links: List[str] = []
    attachments: List[Attachment] = []

def create_http_session():
    session = requests.Session()
    adapter = requests.adapters.HTTPAdapter(
        pool_connections=20,
        pool_maxsize=20,
        max_retries=3
    )
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session

def extract_emails(header_value):
    """Extract email addresses from a header value."""
    return [email for _, email in getaddresses([header_value or ""]) if email]

def extract_links_from_html(html):
    """Extract all links from HTML content."""
    soup = BeautifulSoup(html, "html.parser")
    return [a["href"] for a in soup.find_all("a", href=True)]

def resolve_redirect(link, timeout: int = 3):
    """Resolve a URL to its final destination after following redirects."""
    session = create_http_session()
    try:
        resp = session.head(link, allow_redirects=True, timeout=(1.5, timeout), stream=True)
        return resp.url
    except Exception:
        return link

def deduplicate_and_resolve_links(links):
    """Deduplicate and resolve redirects for a list of URLs."""
    if not links:
        return []
    
    with ThreadPoolExecutor(max_workers=min(10, len(links))) as executor:
        future_to_link = {executor.submit(resolve_redirect, link): link for link in links}
        
        resolved_links: Dict[str, str] = {}
        for future in as_completed(future_to_link):
            original_link = future_to_link[future]
            try:
                resolved_link = future.result()
                resolved_links[original_link] = resolved_link
            except Exception:
                resolved_links[original_link] = original_link
    
    seen = set()
    result = []
    for original, resolved in resolved_links.items():
        normalized = urlparse(resolved)._replace(fragment="").geturl()
        if normalized not in seen:
            seen.add(normalized)
            result.append(normalized)
    
    return result

def clean_text(text: Optional[str]) -> Optional[str]:
    """Clean and normalize text content."""
    if not text:
        return text

    text = html.unescape(text)
    text = re.sub(r'https?://\S+|www\.\S+', '', text)
    text = re.sub(r'[\r\n\t]', ' ', text)
    text = re.sub(r'\s+', ' ', text)
    text = ''.join(c for c in text if c.isprintable())

    return text.strip()

def parse_email_parts(msg):
    """Parse the different parts of an email message."""
    subject = msg.get("Subject")
    if subject:
        subject = clean_text(subject)

    text_body = None
    html_body = None
    links: List[str] = []
    attachments: List[Attachment] = []
    
    for part in msg.walk():
        if part.get_content_maintype() == "multipart":
            continue
        
        ctype = part.get_content_type()
        charset = part.get_content_charset()
        payload = part.get_payload(decode=True)
        size = len(payload) if payload else 0
        
        if not charset and payload:
            charset = chardet.detect(payload)["encoding"]
        
        try:
            content = payload.decode(charset or "utf-8", errors="ignore") if payload else None
        except Exception:
            content = None
        
        if ctype == "text/plain" and content:
            content = clean_text(content)
            if not text_body or len(content) > len(text_body):
                text_body = content
        elif ctype == "text/html" and content and (not html_body or len(content) > len(html_body)):
            html_body = content
            links.extend(extract_links_from_html(html_body))
        
        filename = part.get_filename()
        if filename:
            attachments.append(Attachment(
                filename=filename,
                content_type=ctype,
                size=size,
                content_id=part.get("Content-ID")
            ))
    
    return subject, text_body, html_body, links, attachments

def parse_eml_to_model(raw_email_bytes):
    """Parse raw email bytes into an EmailMessageModel."""
    msg = message_from_bytes(raw_email_bytes)
    
    subject, text_body, html_body, links, attachments = parse_email_parts(msg)
    
    return EmailMessageModel(
        message_id=msg.get("Message-ID", ""),
        subject=subject,
        sender=extract_emails(msg.get("From"))[0] if msg.get("From") else None,
        to=extract_emails(msg.get("To")),
        date=parsedate_to_datetime(msg.get("Date")) if msg.get("Date") else None,
        text_body=text_body,
        html_body=html_body,
        links=deduplicate_and_resolve_links(links),
        attachments=attachments
    )