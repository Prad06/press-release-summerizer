from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys
import os
import logging
import base64
from email import message_from_bytes

# Ensure custom services can be imported
sys.path.append("/opt/src")

from services.google import GmailService
from services.db import DBService

# Setup logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def download_emails_by_ids(**context):
    """
    Downloads raw Gmail emails and saves them to local disk.
    Also extracts and stores attachments and forwarded emails.
    """
    logger.info("Downloading emails by IDs...")

    db_service = DBService()
    gmail_service = GmailService(db_service)

    email_address = context["params"].get("email_address")
    message_ids = context["params"].get("message_ids")

    logger.info(f"Target email: {email_address}")

    for message_id in message_ids:
        logger.info(f"Fetching email with ID: {message_id}")
        email = gmail_service.get_message(message_id, format="raw")
        message_dir = f"/tmp/{message_id}"
        attachments_dir = os.path.join(message_dir, "attachments")

        os.makedirs(message_dir, exist_ok=True)
        os.makedirs(attachments_dir, exist_ok=True)

        email_path = os.path.join(message_dir, "originaltext")

        if "raw" not in email:
            logger.warning(f"No raw content found for email ID: {message_id}")
            continue

        raw_email_bytes = base64.urlsafe_b64decode(email["raw"])
        
        # Save the raw email to disk
        with open(email_path, "wb") as f:
            f.write(raw_email_bytes)

        logger.info(f"Saved raw email to: {email_path}")

        # Parse the email and extract attachments and forwards
        email_message = message_from_bytes(raw_email_bytes)

        for part in email_message.walk():
            content_type = part.get_content_type()
            filename = part.get_filename()

            if part.get_content_maintype() == "multipart":
                continue

            # Save attachments or embedded forwarded emails
            if filename or content_type == "message/rfc822":
                if not filename:
                    filename = f"forwarded_email_{message_id}.eml"

                filepath = os.path.join(attachments_dir, filename)
                with open(filepath, "wb") as af:
                    af.write(part.get_payload(decode=True))

                logger.info(f"Saved attachment/forward: {filename}")


with DAG(
    dag_id="download_raw_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["raw", "gmail", "download"],
    params={
        "email_address": "",
        "message_ids": [],
    },
) as dag:

    download_email = PythonOperator(
        task_id="download_email",
        python_callable=download_emails_by_ids,
        provide_context=True,
    )

    download_email
