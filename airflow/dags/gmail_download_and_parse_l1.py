from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime
import os
import sys
import base64
import logging
import json

sys.path.append("/opt/src")

from services.google import GmailService
from services.db import DBService
from parsers.email import parse_eml_to_model

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

DATA_PATH = os.getenv("DATA_PATH")

def write_file(path: str, data: bytes, mode: str = "wb"):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, mode) as f:
        f.write(data)
    logger.info(f"Wrote data to: {path}")

def save_l0(**context):
    """
    Fetch raw emails and save to {DATA_PATH}/<message_id>/l0/email.eml.
    Push successful message_ids to XCom under 'l0_successful_ids'.
    """
    email_address = context["params"]["email_address"]
    message_ids = context["params"]["message_ids"]

    db_service = DBService()
    gmail_service = GmailService(db_service)

    successful_ids = []

    for message_id in message_ids:
        try:
            logger.info(f"Fetching email with ID: {message_id}")
            email = gmail_service.get_message(message_id, format="raw")

            if "raw" not in email:
                raise ValueError("No raw content in email.")

            raw_bytes = base64.urlsafe_b64decode(email["raw"])
            eml_path = f"{DATA_PATH}/{message_id}/l0/email.eml"
            write_file(eml_path, raw_bytes)

            retrieved_timestamp = datetime.now().astimezone().isoformat()
            ts_path = f"{DATA_PATH}/{message_id}/l0/ts.json"
            write_file(ts_path, json.dumps({"retrieved_timestamp": retrieved_timestamp}, indent=2).encode("utf-8"))

            logger.info(f"Saved L0 email to {eml_path}")
            successful_ids.append(message_id)

        except Exception as e:
            logger.error(f"L0 failure for {message_id}: {e}")

    context["ti"].xcom_push(key="l0_successful_ids", value=successful_ids)

def parse_and_save_l1(**context):
    """
    Read .eml files from {DATA_PATH}/l0 and save JSON to {DATA_PATH}/l1/<message_id>/email.json.
    Push successful message_ids to XCom under 'l1_successful_ids'.
    """
    ti = context["ti"]
    l0_successful_ids = ti.xcom_pull(task_ids="save_l0", key="l0_successful_ids") or []

    l1_successful_ids = []

    for message_id in l0_successful_ids:
        try:
            eml_path = f"{DATA_PATH}/{message_id}/l0/email.eml"
            with open(eml_path, "rb") as f:
                raw_bytes = f.read()

            parsed = parse_eml_to_model(raw_bytes)

            json_path = f"{DATA_PATH}/{message_id}/l1/email.json"
            write_file(
                json_path,
                json.dumps(parsed.model_dump(), indent=2, default=str).encode("utf-8")
            )

            logger.info(f"Saved L1 parsed email to {json_path}")
            l1_successful_ids.append(message_id)

        except Exception as e:
            logger.error(f"L1 failure for {message_id}: {e}")

    ti.xcom_push(key="l1_successful_ids", value=l1_successful_ids)

def trigger_l2_dag(**context):
    """Trigger the L2 pipeline using TriggerDagRunOperator programmatically."""
    logger.info("Starting trigger_l2_dag")

    try:
        ti = context["ti"]
        l1_successful_ids = ti.xcom_pull(task_ids="parse_and_save_l1", key="l1_successful_ids") or []

        if not l1_successful_ids:
            logger.warning("No successful message IDs to trigger L2 DAG.")
            return

        conf = {"message_ids": l1_successful_ids}

        trigger_task = TriggerDagRunOperator(
            task_id="trigger_find_news_release_link_l2",
            trigger_dag_id="find_news_release_link_l2",
            conf=conf,
            reset_dag_run=True,
            wait_for_completion=False,
        )

        trigger_task.execute(context=context)

    except Exception as e:
        logger.error(f"Error in trigger_l2_dag: {e}")
        raise

    finally:
        logger.info("Finished trigger_l2_dag")


with DAG(
    dag_id="gmail_download_and_parse_l1",
    start_date=datetime(2025, 5, 12),
    schedule_interval=None,
    catchup=False,
    tags=["gmail", "download", "parse"],
    params={
        "email_address": "",
        "message_ids": [],
    },
) as dag:

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    save_l0_task = PythonOperator(
        task_id="save_l0",
        python_callable=save_l0,
        provide_context=True,
    )

    parse_l1_task = PythonOperator(
        task_id="parse_and_save_l1",
        python_callable=parse_and_save_l1,
        provide_context=True,
    )

    trigger_l2_task = PythonOperator(
        task_id="trigger_l2_dag",
        python_callable=trigger_l2_dag,
        provide_context=True,
    )

    start >> save_l0_task >> parse_l1_task >> trigger_l2_task >> end
