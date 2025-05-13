import os
import logging
import requests
from requests.auth import HTTPBasicAuth

logger = logging.getLogger(__name__)

class AirflowTrigger:
    def __init__(self):
        self.api_url = os.environ.get("AIRFLOW_API_URL")
        self.username = os.environ.get("AIRFLOW_USERNAME")
        self.password = os.environ.get("AIRFLOW_PASSWORD")

        if not self.api_url or not self.username or not self.password:
            logger.error("Missing Airflow credentials or URL.")
            raise ValueError("Missing Airflow credentials or URL.")

    def trigger_dag(self, dag_id: str, **conf):
        """
        Triggers an Airflow DAG with the given configuration.

        :param dag_id: ID of the DAG to trigger
        :param conf: Dictionary of parameters to pass as DAG run config
        :return: Dictionary with status and response details
        """
        dag_trigger_url = f"{self.api_url}/dags/{dag_id}/dagRuns"
        logger.info(f"Triggering DAG: {dag_id} at {dag_trigger_url}")
        payload = {"conf": conf}

        logger.info(f"Triggering DAG '{dag_id}' at {dag_trigger_url}")
        logger.info(f"Payload: {payload}")

        try:
            response = requests.post(
                dag_trigger_url,
                json=payload,
                auth=HTTPBasicAuth(self.username, self.password),
                headers={"Content-Type": "application/json"},
                timeout=20,
            )

            logger.info(f"Airflow response: {response.status_code} {response.text}")

            if response.status_code in (200, 201):
                run_id = response.json().get("dag_run_id", "N/A")
                logger.info(f"DAG {dag_id} triggered successfully with conf {conf}")
                return {
                    "status": "success",
                    "dag_run_id": run_id,
                    "message": f"DAG {dag_id} triggered successfully",
                }
            else:
                logger.error(f"Failed to trigger DAG: {response.text}")
                return {
                    "status": "error",
                    "message": f"Failed to trigger DAG. Status: {response.status_code}",
                    "error": response.text,
                }

        except requests.exceptions.RequestException as e:
            logger.error(f"Request to trigger DAG failed: {e}")
            return {
                "status": "error",
                "message": "Exception occurred while triggering DAG",
                "error": str(e),
            }
