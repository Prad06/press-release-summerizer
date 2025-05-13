import os
import logging
import requests
from requests.auth import HTTPBasicAuth

logger = logging.getLogger(__name__)

def trigger_airflow_dag(dag_id: str, email_address: str, message_ids: list):
    """
    Trigger an Airflow DAG run via the stable REST API.
    
    :param dag_id: The DAG ID to trigger
    :param email_address: Email address to pass as DAG config
    :param user_id: Optional user identifier for logging
    :return: Response object or error
    """
    AIRFLOW_API_URL = os.environ.get("AIRFLOW_API_URL")
    AIRFLOW_USERNAME = os.environ.get("AIRFLOW_USERNAME")
    AIRFLOW_PASSWORD = os.environ.get("AIRFLOW_PASSWORD")

    if not AIRFLOW_API_URL or not AIRFLOW_USERNAME or not AIRFLOW_PASSWORD:
        logging.error("Missing Airflow credentials or URL.")
        return None

    dag_trigger_url = f"{AIRFLOW_API_URL}/dags/{dag_id}/dagRuns"
    payload = {
        "conf": {
            "message_ids": message_ids,
            "email_address": email_address
        }
    }

    logging.info(f"Triggering DAG '{dag_id}' at {dag_trigger_url}")
    logging.info(f"Payload: {payload}")

    try:
        response = requests.post(
            dag_trigger_url,
            json=payload,
            auth=HTTPBasicAuth(AIRFLOW_USERNAME, AIRFLOW_PASSWORD),
            headers={"Content-Type": "application/json"},
            timeout=10,
        )

        logging.info(f"Airflow response: {response.status_code} {response.text}")

        if response.status_code in (200, 201):
            run_id = response.json().get("dag_run_id", "N/A")
            logging.info(f"DAG {dag_id} triggered successfully for {email_address}")
            return {
                "status": "success",
                "dag_run_id": run_id,
                "message": f"DAG {dag_id} triggered",
            }
        else:
            logging.error(f"Failed to trigger DAG: {response.text}")
            return {
                "status": "error",
                "message": f"Failed to trigger DAG. Status: {response.status_code}",
                "error": response.text
            }

    except requests.exceptions.RequestException as e:
        logging.error(f"Request to trigger DAG failed: {e}")
        return {
            "status": "error",
            "message": "Exception occurred while triggering DAG",
            "error": str(e)
        }
