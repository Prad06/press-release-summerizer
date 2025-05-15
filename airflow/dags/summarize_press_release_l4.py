from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime
import os
import json
import logging
import tiktoken
import sys
import pytz
from dateutil import parser

# Mount src for custom modules
sys.path.append("/opt/src")
from chat import Chat, ChatConfig

# Logger setup
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

DATA_PATH = os.environ.get("DATA_PATH")

# Tokenizer setup for GPT-4o
tokenizer = tiktoken.encoding_for_model("gpt-4o")
MAX_TOKENS_PER_CHUNK = 4096

# Eastern timezone for consistent timestamps
eastern_tz = pytz.timezone("US/Eastern")

def read_file(path):
    """Read text content from file."""
    try:
        with open(path, "r", encoding="utf-8") as f:
            return f.read()
    except Exception as e:
        logger.error(f"Failed to read file {path}: {e}")
        return ""

def read_json(path):
    """Read JSON content from file."""
    try:
        with open(path) as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"Failed to read JSON file {path}: {e}")
        return {}

def chunk_text(text):
    """Split text into chunks based on max token size."""
    tokens = tokenizer.encode(text)
    return [tokenizer.decode(tokens[i:i + MAX_TOKENS_PER_CHUNK]) for i in range(0, len(tokens), MAX_TOKENS_PER_CHUNK)]

def convert_to_eastern(timestamp_str):
    """Convert a timestamp string to US/Eastern timezone."""
    if not timestamp_str or timestamp_str == "Unknown":
        return "Unknown"
    try:
        dt = parser.parse(timestamp_str)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=pytz.UTC)
        return dt.astimezone(eastern_tz).strftime("%Y-%m-%d %H:%M:%S %Z")
    except Exception as e:
        logger.error(f"Error converting timestamp {timestamp_str}: {e}")
        return f"{timestamp_str} (conversion error)"

def summarize_release(**context):
    """Generate structured and short summaries using LLM for each message ID."""
    summary_ts = datetime.now(eastern_tz).strftime("%Y-%m-%d %H:%M:%S %Z")
    extraction_summary = context["params"].get("extraction_summary", {})
    chat = Chat(ChatConfig(model="gpt-4o", temperature=0, max_tokens=1024))

    for msg_id in extraction_summary:
        try:
            logger.info(f"Summarizing release for: {msg_id}")
            base_path = f"{DATA_PATH}"

            # Load L1 and L3 data
            l1 = read_json(f"{base_path}/l1/{msg_id}/email.json")
            l3 = read_file(f"{base_path}/l3/{msg_id}/main_text.txt")
            ts = read_file(f"{base_path}/l3/{msg_id}/press_release_timestamp.txt").strip()
            pdfs = read_json(f"{base_path}/l3/{msg_id}/pdf_links.json")

            # Extract fields
            subject = l1.get("subject", "")
            sender = l1.get("sender", "")
            email_ts = l1.get("date", "")
            url = l1.get("links", [None])[0]
            body = l1.get("text_body", "")

            # Convert timestamps to Eastern Time
            email_ts_eastern = convert_to_eastern(email_ts)
            pr_ts_eastern = convert_to_eastern(ts) if ts else "Unknown"
            retrieval_ts = datetime.now(eastern_tz).strftime("%Y-%m-%d %H:%M:%S %Z")

            # Combine and chunk text for summarization
            combined_text = f"Subject: {subject}\n\nEmail Body:\n{body}\n\nPress Release Page:\n{l3}"
            chunks = chunk_text(combined_text)

            # Prompt and summarize
            system_prompt = """
You are an expert biotech analyst summarizing press releases for institutional investors and researchers. Your task is to extract the most important information with precision and domain knowledge.

REQUIRED OUTPUT STRUCTURE:
1. HEADLINE: Concise 1-line description of the key announcement (max 15 words)
2. ANNOUNCEMENT TYPE: Categorize as one of [Clinical Trial Results, Regulatory Update, Financial Results, Partnership/Licensing, Product Launch, Management Change, Corporate Development, Scientific Publication]
3. COMPANY: Company name and ticker symbol if mentioned
4. KEY DRUGS/PRODUCTS: Names of all drugs/products mentioned with their development stage and indication (e.g., "DRUG-123 (Phase 2, breast cancer)")
5. CLINICAL DATA: For trial results, extract specific efficacy metrics (e.g., ORR%, PFS, OS) and p-values if available
6. REGULATORY STATUS: Any FDA, EMA or other regulatory updates with specific timelines
7. FINANCIAL IMPACT: Financial details if mentioned (e.g., deal size, milestone payments, expected revenue)
8. NEXT STEPS: Clearly stated next developments or milestones with timelines
9. CONTEXT: Brief additional context that would be relevant to investors or researchers

IMPORTANT GUIDELINES:
- Be extremely precise with drug names, numerical values, and scientific terminology
- Include all specific percentages, p-values, and statistical data when available
- Maintain proper capitalization of drug names and company names
- If information for a category is not available, write "Not mentioned" for that category
- For ambiguous/unclear information, indicate uncertainty rather than making assumptions
- Preserve the exact timing information provided (e.g., "expected in Q4 2025" not just "later this year")
- Focus only on facts directly stated in the press release, not implications or predictions
"""
            summaries = [chat.ask(system_prompt, chunk) for chunk in chunks]
            full_summary = "\n\n".join(summaries)

            short_prompt = """
Create a concise 2-3 sentence summary of this biotech press release that an investor would find immediately useful. Include: (1) the main announcement, (2) the most significant data point or business impact, and (3) the next key milestone if mentioned. Use precise terminology, exact percentages/values, and maintain scientific accuracy. Be direct and concise without sacrificing critical details.
"""
            email_summary = chat.ask(short_prompt, full_summary)

            # Prepare output directory and save
            out_dir = f"{base_path}/l4/{msg_id}"
            os.makedirs(out_dir, exist_ok=True)

            with open(f"{out_dir}/page_summary.txt", "w") as f:
                f.write(full_summary)
            with open(f"{out_dir}/email_summary.txt", "w") as f:
                f.write(email_summary)

            final_json = {
                "message_id": msg_id,
                "email_subject": subject,
                "email_sender": sender,
                "email_timestamp": email_ts_eastern,
                "press_release_url": url,
                "press_release_website_timestamp": pr_ts_eastern,
                "retrieval_timestamp": retrieval_ts,
                "summary_timestamp": summary_ts,
                "press_release_text": l3,
                "llm_summary": full_summary,
                "email_summary": email_summary,
                "pdf_links": pdfs
            }

            with open(f"{out_dir}/final_output.json", "w") as f:
                json.dump(final_json, f, indent=2)

            logger.info(f"Stored summary files for {msg_id}")

        except Exception as e:
            logger.error(f"Failed summarizing {msg_id}: {e}")

def create_report(**context):
    """Generate plain text and Markdown reports from LLM summaries."""
    summary = context["params"].get("extraction_summary", {})

    for msg_id in summary:
        try:
            path = f"{DATA_PATH}/l4/{msg_id}"
            data = read_json(f"{path}/final_output.json")
            email_summary = data.get("email_summary", "No summary available.")

            # Extract timestamps and metadata
            email_timestamp = data.get("email_timestamp", "Unknown")
            pr_website_timestamp = data.get("press_release_website_timestamp", "Unknown")
            retrieval_timestamp = data.get("retrieval_timestamp", "Unknown")
            summary_timestamp = data.get("summary_timestamp", "Unknown")

            # Prepare plain text report
            report = f"""BIOTECH PRESS RELEASE DIGEST

Subject: {data.get("email_subject", "No subject")}
From: {data.get("email_sender", "Unknown sender")}

TIMESTAMPS (Eastern Time):
- Email Received: {email_timestamp}
- Press Release Published: {pr_website_timestamp}
- Content Retrieved: {retrieval_timestamp}
- Summary Generated: {summary_timestamp}

URL: {data.get("press_release_url", "No URL")}

EXECUTIVE SUMMARY:
{email_summary}

RESOURCES:
"""
            pdf_links = data.get("pdf_links", [])
            report += "\n".join([f"- {p}" for p in pdf_links]) if pdf_links else "- No supplementary documents available"

            with open(f"{path}/summary_report.txt", "w") as f:
                f.write(report)

            # Prepare Markdown report
            md_report = f"""# Biotech Press Release Summary

## Basic Information
- **Subject:** {data.get("email_subject", "No subject")}
- **Source:** {data.get("email_sender", "Unknown sender")}

## Timestamps (Eastern Time)
- **Email Received:** {email_timestamp}
- **Press Release Published:** {pr_website_timestamp}
- **Content Retrieved:** {retrieval_timestamp}
- **Summary Generated:** {summary_timestamp}

- **URL:** {data.get("press_release_url", "No URL")}

## Executive Summary
{email_summary}

## Resources
"""
            md_report += "\n".join([f"- [{os.path.basename(link) or 'Document'}]({link})" for link in pdf_links]) if pdf_links else "- No supplementary documents available\n"

            with open(f"{path}/summary_report.md", "w") as f:
                f.write(md_report)

            logger.info(f"Report created for {msg_id}")

        except Exception as e:
            logger.error(f"Report creation failed for {msg_id}: {e}")

def send_report(**context):
    """Placeholder for sending report via email, Slack, etc."""
    logger.info("Stub: implement notification/email/Slack upload here if needed.")

with DAG(
    dag_id="summarize_press_release_l4",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["summerize", "report"],
    params={"extraction_summary": {}},
) as dag:

    start = EmptyOperator(task_id="start")

    summarize = PythonOperator(
        task_id="summarize_release",
        python_callable=summarize_release,
        provide_context=True
    )

    report = PythonOperator(
        task_id="create_report",
        python_callable=create_report,
        provide_context=True
    )

    send = PythonOperator(
        task_id="send_report",
        python_callable=send_report,
        provide_context=True
    )

    end = EmptyOperator(task_id="end")

    start >> summarize >> report >> send >> end
