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
import requests
from PyPDF2 import PdfReader
from urllib.parse import urljoin

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

DATA_PATH = os.environ.get("DATA_PATH")

def normalize_url(pdf_url: str, base_url: str = None) -> str:
    """
    Normalize URL by handling various formats and edge cases.

    :param pdf_url: The URL to normalize.
    :param base_url: The base URL to use for relative URLs.
    :return: A normalized URL or None if the input is invalid.
    """
    if not pdf_url or pdf_url.isspace():
        return None

    pdf_url = pdf_url.strip()
    if pdf_url.startswith("//"):
        return "https:" + pdf_url

    if pdf_url.startswith(("http://", "https://")):
        return pdf_url
    if base_url:
        return urljoin(base_url, pdf_url)
    
    return "https://" + pdf_url

def extract_html_with_selenium(**context):
    """
    Launch headless Chrome using Selenium to fetch full HTML from all message_links.
    Save raw HTML to disk and push HTML content to XCom.
    """
    logger.info("Starting extract_html_with_selenium")
    message_links = context["params"].get("message_links", {})
    ti = context["ti"]

    chrome_path = os.environ.get("CHROME_BIN")
    chromedriver_path = os.environ.get("CHROMEDRIVER_BIN")

    options = Options()
    options.binary_location = chrome_path
    options.add_argument("--headless")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("user-agent=Mozilla/5.0")

    service = Service(executable_path=chromedriver_path)
    driver = webdriver.Chrome(service=service, options=options)

    html_contents = {}
    actual_urls = {}

    for msg_id, url in message_links.items():
        try:
            logger.info(f"Fetching HTML for {msg_id} from {url}")
            driver.get(url)
            html = driver.page_source
            actual_url = driver.current_url
            
            html_contents[msg_id] = html
            actual_urls[msg_id] = actual_url
            
            os.makedirs(f"{DATA_PATH}/l3/{msg_id}", exist_ok=True)
        except Exception as e:
            logger.error(f"Failed to fetch HTML for {msg_id}: {e}")

    driver.quit()
    logger.info("Selenium driver closed.")
    
    ti.xcom_push(key="html_contents", value=html_contents)
    ti.xcom_push(key="actual_urls", value=actual_urls)
    
    return list(html_contents.keys())

def extract_main_text(html):
    """
    Use Trafilatura to extract the main article content from full HTML.
    """
    try:
        return trafilatura.extract(html) or ""
    except Exception as e:
        logger.error(f"Trafilatura failed: {e}")
        return ""

def extract_pdf_links(soup):
    """
    Find all candidate PDF links from HTML by inspecting tag attributes.
    """
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
        return list(pdf_links)
    except Exception as e:
        logger.error(f"Error while extracting PDF links: {e}")
        return []

def process_and_save_extractions(**context):
    """
    Process HTML from XCom: extract main text and PDF links, save outputs to disk.
    """
    logger.info("Starting process_and_save_extractions")
    ti = context["ti"]
    html_contents = ti.xcom_pull(task_ids="extract_html_with_selenium", key="html_contents")
    actual_urls = ti.xcom_pull(task_ids="extract_html_with_selenium", key="actual_urls") or {}

    summary = {}

    for msg_id, html in (html_contents or {}).items():
        try:
            base_url = actual_urls.get(msg_id, "")
            soup = BeautifulSoup(html, "html.parser")
            main_text = extract_main_text(html)
            pdf_links = extract_pdf_links(soup)

            output_dir = f"{DATA_PATH}/l3/{msg_id}"
            os.makedirs(output_dir, exist_ok=True)

            with open(f"{output_dir}/raw.html", "w", encoding="utf-8") as f:
                f.write(html)

            with open(f"{output_dir}/main_text.txt", "w", encoding="utf-8") as f:
                f.write(main_text)

            with open(f"{output_dir}/pdf_links.json", "w") as f:
                json.dump({"links": pdf_links, "base_url": base_url}, f, indent=2)

            summary[msg_id] = {
                "has_main_text": bool(main_text),
                "pdf_links_count": len(pdf_links),
                "output_dir": output_dir,
                "base_url": base_url
            }

            logger.info(f"Extraction completed for {msg_id}")

        except Exception as e:
            logger.error(f"Failed to process {msg_id}: {e}")

    ti.xcom_push(key="extraction_summary", value=summary)
    return summary

def extract_pdfs(**context):
    """
    Download and extract text from each PDF found in pdf_links.json.
    Save full content and per-file summaries to the pdf_summary.json file.
    """
    logger.info("Starting extract_pdfs")
    ti = context["ti"]
    extraction_summary = ti.xcom_pull(task_ids="process_and_save_extractions", key="extraction_summary") or {}

    for msg_id, meta in extraction_summary.items():
        try:
            output_dir = meta.get("output_dir")
            base_url = meta.get("base_url", "")
            pdf_dir = os.path.join(output_dir, "pdfs")
            os.makedirs(pdf_dir, exist_ok=True)

            pdf_links_path = os.path.join(output_dir, "pdf_links.json")
            if not os.path.exists(pdf_links_path):
                logger.warning(f"No PDF links for {msg_id}")
                continue

            with open(pdf_links_path, "r") as f:
                pdf_data = json.load(f)
                raw_links = pdf_data.get("links", [])
                base_url = pdf_data.get("base_url", base_url)

            pdf_links = [normalize_url(url, base_url) for url in raw_links]
            pdf_links = [url for url in pdf_links if url]

            pdf_summary = []

            for idx, pdf_url in enumerate(pdf_links):
                try:
                    filename = f"pdf_{idx + 1}.pdf"
                    pdf_path = os.path.join(pdf_dir, filename)

                    logger.info(f"Downloading {pdf_url}")
                    r = requests.get(pdf_url, stream=True, timeout=30)
                    r.raise_for_status()
                    with open(pdf_path, "wb") as f_out:
                        for chunk in r.iter_content(chunk_size=8192):
                            f_out.write(chunk)

                    reader = PdfReader(pdf_path)
                    pages = [p.extract_text() or "" for p in reader.pages]

                    pdf_summary.append({
                        "url": pdf_url,
                        "filename": filename,
                        "num_pages": len(pages),
                        "text": pages,
                        "error": None
                    })

                except Exception as e:
                    logger.error(f"PDF extraction failed: {pdf_url} - {e}")
                    pdf_summary.append({
                        "url": pdf_url,
                        "filename": None,
                        "num_pages": 0,
                        "text": [],
                        "error": str(e)
                    })

            with open(os.path.join(pdf_dir, "pdf_summary.json"), "w", encoding="utf-8") as f:
                json.dump(pdf_summary, f, indent=2)

            logger.info(f"Finished PDF extraction for {msg_id}")

        except Exception as e:
            logger.error(f"Unexpected failure for {msg_id}: {e}")

def trigger_l4_dag(**context):
    """
    Programmatically trigger the L4 DAG after all extraction is complete.
    """
    logger.info("Triggering summarize_press_release_l4 DAG")
    try:
        ti = context["ti"]
        extraction_summary = ti.xcom_pull(task_ids="process_and_save_extractions", key="extraction_summary") or {}

        if not extraction_summary:
            logger.warning("No summary to trigger downstream DAG.")
            return

        conf = {"extraction_summary": extraction_summary}

        trigger = TriggerDagRunOperator(
            task_id="trigger_l4_task_internal",
            trigger_dag_id="summarize_press_release_l4",
            conf=conf,
            wait_for_completion=False,
        )

        trigger.execute(context=context)
        logger.info("L4 DAG triggered")

    except Exception as e:
        logger.error(f"Triggering L4 DAG failed: {e}")
        raise

with DAG(
    dag_id="extract_release_information_l3",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["press-release", "extraction"],
    params={"message_links": {}},
) as dag:

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

    extract_pdf_files = PythonOperator(
        task_id="extract_pdfs",
        python_callable=extract_pdfs,
        provide_context=True,
    )

    trigger_l4 = PythonOperator(
        task_id="trigger_l4_dag",
        python_callable=trigger_l4_dag,
        provide_context=True,
    )

    start >> extract_html >> process_extractions >> extract_pdf_files >> trigger_l4 >> end