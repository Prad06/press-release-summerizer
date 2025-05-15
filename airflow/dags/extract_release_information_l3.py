from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from datetime import datetime
import os
import json
import logging
from bs4 import BeautifulSoup
import trafilatura
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from dateutil import parser
import pytz

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

DATA_PATH = os.environ.get("DATA_PATH")

def extract_html_with_selenium(**context):
    """
    Extract full HTML using Selenium and save to {DATA_PATH}/l3/<message_id>/raw.html.
    """
    logger.info("Starting extract_html_with_selenium function")
    message_links = context["params"].get("message_links", {})
    ti = context["ti"]

    # Get Chrome binary and Chromedriver paths from environment variables
    chrome_path = os.environ.get("CHROME_BIN")
    chromedriver_path = os.environ.get("CHROMEDRIVER_BIN")

    logger.debug(f"Chrome binary path: {chrome_path}")
    logger.debug(f"Chromedriver binary path: {chromedriver_path}")

    # Configure Selenium Chrome options
    options = Options()
    options.binary_location = chrome_path
    options.add_argument("--headless=new")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--window-size=1920,1080")

    # Initialize Selenium WebDriver
    driver = webdriver.Chrome(service=Service(chromedriver_path), options=options)

    html_contents = {}

    # Iterate through message links and fetch HTML
    for msg_id, url in message_links.items():
        try:
            logger.info(f"Fetching HTML for message ID: {msg_id} with URL: {url}")
            driver.get(url)
            html = driver.page_source

            html_contents[msg_id] = html

            # Create output directory for the message ID
            output_dir = f"{DATA_PATH}/l3/{msg_id}"
            os.makedirs(output_dir, exist_ok=True)

            logger.info(f"Successfully fetched HTML for {msg_id}")

        except Exception as e:
            logger.error(f"Failed to fetch HTML for {msg_id}: {e}")

    # Quit the Selenium driver
    driver.quit()
    logger.info("Selenium driver quit successfully")

    # Push HTML contents to XCom
    ti.xcom_push(key="html_contents", value=html_contents)
    logger.info("HTML contents pushed to XCom")
    return list(html_contents.keys())


def extract_main_text(html):
    """
    Extract main content using Trafilatura from HTML.
    """
    logger.info("Starting extract_main_text function")
    try:
        main_text = trafilatura.extract(html)
        logger.debug("Main text extraction completed")
        return main_text or ""
    except Exception as e:
        logger.error(f"Error extracting main text: {e}")
        return ""


def extract_pdf_links(soup):
    """
    Extract PDF links from a BeautifulSoup object.
    """
    logger.info("Starting extract_pdf_links function")
    try:
        pdf_links = set()
        for tag in soup.find_all(["a", "iframe", "embed", "object"]):
            if tag.get("type", "").lower() == "application/pdf":
                pdf_links.add(tag.get("href") or tag.get("src") or "")
            else:
                for attr in ["href", "src", "data"]:
                    val = tag.get(attr, "")
                    if val and ".pdf" in val.lower():
                        pdf_links.add(val)

        logger.debug(f"Extracted PDF links: {pdf_links}")
        return list(pdf_links)
    except Exception as e:
        logger.error(f"Error extracting PDF links: {e}")
        return []


def extract_press_release_timestamp(soup):
    """
    Extract press release timestamp in a simplified manner and return in EST.
    """
    logger.info("Starting extract_press_release_timestamp function")
    try:
        timestamp = None

        # Attempt to extract timestamp from <time> tag
        time_tag = soup.find("time")
        if time_tag and time_tag.get("datetime"):
            timestamp = time_tag["datetime"]
        elif time_tag:
            timestamp = time_tag.get_text(strip=True)

        # Attempt to extract timestamp from meta tags
        if not timestamp:
            for meta in soup.find_all("meta"):
                if meta.get("property") in ["article:published_time", "og:published_time"]:
                    timestamp = meta.get("content")
                    break

        # Parse and format timestamp to EST
        if timestamp:
            parsed_time = parser.parse(timestamp)
            est = pytz.timezone("US/Eastern")
            formatted_time = parsed_time.astimezone(est).strftime("%Y-%m-%d %H:%M:%S")
            logger.debug(f"Extracted timestamp: {formatted_time}")
            return formatted_time

        logger.warning("No timestamp found")
        return ""
    except Exception as e:
        logger.error(f"Error extracting timestamp: {e}")
        return ""


def process_and_save_extractions(**context):
    """
    Process HTML contents from XCom and write all results to disk at once.
    """
    logger.info("Starting process_and_save_extractions function")
    ti = context["ti"]
    html_contents = ti.xcom_pull(task_ids="extract_html_with_selenium", key="html_contents")

    if not html_contents:
        logger.warning("No HTML contents found")
        return

    all_results = {}

    # Process each HTML content
    for msg_id, html in html_contents.items():
        try:
            logger.info(f"Processing extractions for {msg_id}")

            soup = BeautifulSoup(html, "html.parser")

            main_text = extract_main_text(html)
            pdf_links = extract_pdf_links(soup)
            timestamp = extract_press_release_timestamp(soup)

            all_results[msg_id] = {
                "raw_html": html,
                "main_text": main_text,
                "pdf_links": pdf_links,
                "timestamp": timestamp
            }

            logger.info(f"Completed extractions for {msg_id}")

        except Exception as e:
            logger.error(f"Failed to process HTML for {msg_id}: {e}")

    # Save results to disk
    for msg_id, results in all_results.items():
        try:
            output_dir = f"{DATA_PATH}/l3/{msg_id}"

            with open(f"{output_dir}/raw.html", "w", encoding="utf-8") as f:
                f.write(results["raw_html"])

            with open(f"{output_dir}/main_text.txt", "w", encoding="utf-8") as f:
                f.write(results["main_text"])

            with open(f"{output_dir}/pdf_links.json", "w") as f:
                json.dump(results["pdf_links"], f, indent=2)

            with open(f"{output_dir}/press_release_timestamp.txt", "w", encoding="utf-8") as f:
                f.write(results["timestamp"])

            logger.info(f"Successfully wrote all results for {msg_id}")
        except Exception as e:
            logger.error(f"Failed to write results for {msg_id}: {e}")

    # Create a summary of the extractions
    summary = {
        msg_id: {
            "has_main_text": bool(results["main_text"]),
            "pdf_links_count": len(results["pdf_links"]),
            "has_timestamp": bool(results["timestamp"])
        }
        for msg_id, results in all_results.items()
    }
    ti.xcom_push(key="extraction_summary", value=summary)
    logger.info("Extraction summary pushed to XCom")

    return summary


def trigger_l4_dag(**context):
    """
    Trigger the L4 pipeline using TriggerDagRunOperator programmatically.
    """
    try:
        ti = context["ti"]
        extraction_summary = ti.xcom_pull(task_ids="process_and_save_extractions", key="extraction_summary") or {}

        if not extraction_summary:
            logger.warning("No extraction summary to trigger L4 DAG.")
            return

        conf = {"extraction_summary": extraction_summary}
        logger.debug(f"Triggering L4 DAG with config: {conf}")

        # Trigger the L4 DAG
        trigger_task = TriggerDagRunOperator(
            task_id="trigger_l4_task",
            trigger_dag_id="summerize_press_release_l4",
            conf=conf,
            wait_for_completion=True,
        )

        trigger_task.execute(context=context)
        logger.info("L4 DAG triggered successfully")

    except Exception as e:
        logger.error(f"Failed to trigger L4 DAG: {e}")
        raise


# Define the DAG
with DAG(
    dag_id="extract_release_information_l3",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["press-release", "extraction"],
    params={"message_links": {}},
) as dag:

    # Define tasks
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    extract_html = PythonOperator(
        task_id="extract_html_with_selenium",
        python_callable=extract_html_with_selenium,
        provide_context=True,
    )

    process_extractions = PythonOperator(
        task_id="process_and_save_extractions",
        python_callable=process_and_save_extractions,
        provide_context=True,
    )

    trigger_l4_task = PythonOperator(
        task_id="trigger_l4_dag",
        python_callable=trigger_l4_dag,
        provide_context=True,
    )

    # Define task dependencies
    start >> extract_html >> process_extractions >> trigger_l4_task >> end