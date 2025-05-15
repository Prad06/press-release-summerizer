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
import re
from datetime import datetime
from dateutil import parser

# Mount src for custom modules
sys.path.append("/opt/src")
from chat import Chat, ChatConfig
from services.db import DBService

# Logger setup
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

DATA_PATH = os.environ.get("DATA_PATH")

# Tokenizer setup for GPT-4o
tokenizer = tiktoken.encoding_for_model("gpt-4o")
MAX_TOKENS_PER_CHUNK = 4096

# Eastern timezone for consistent timestamps
eastern_tz = pytz.timezone("US/Eastern")

SYSTEM_PROMPT = """
You are an expert biotech analyst summarizing press releases for institutional investors and researchers. Your task is to extract the most important information with precision and domain knowledge.

REQUIRED OUTPUT STRUCTURE (Use exactly this format with headers):
1. HEADLINE: Concise 1-line description of the key announcement (max 15 words)
2. ANNOUNCEMENT TYPE: Categorize as one of [Clinical Trial Results, Regulatory Update, Financial Results, Partnership/Licensing, Product Launch, Management Change, Corporate Development, Scientific Publication]
3. COMPANY: Company name and ticker symbol if mentioned
4. KEY DRUGS/PRODUCTS: Names of all drugs/products mentioned with their development stage and indication (e.g., "DRUG-123 (Phase 2, breast cancer)")
5. CLINICAL DATA: For trial results, extract specific efficacy metrics (e.g., ORR%, PFS, OS) and p-values if available
6. REGULATORY STATUS: Any FDA, EMA or other regulatory updates with specific timelines
7. FINANCIAL IMPACT: Financial details if mentioned (e.g., deal size, milestone payments, expected revenue)
8. NEXT STEPS: Clearly stated next developments or milestones with timelines
9. CONTEXT: Brief additional context that would be relevant to investors or researchers
10. RELEASE TIMESTAMP: Extract the exact date and time when the press release was officially issued. Format as: YYYY-MM-DD HH:MM TZ

IMPORTANT GUIDELINES:
- Focus primarily on the press release content, not the email that delivered it
- Be extremely precise with drug names, numerical values, and scientific terminology
- Include all specific percentages, p-values, and statistical data when available
- Maintain proper capitalization of drug names and company names
- If information for a category is not available, write "Not mentioned" for that category
- For ambiguous/unclear information, indicate uncertainty rather than making assumptions
- Preserve the exact timing information provided (e.g., "expected in Q4 2025" not just "later this year")
- Focus only on facts directly stated in the press release, not implications or predictions
- Pay close attention to the release timestamp, usually found at the beginning or end of the press release
"""

SHORT_SYSTEM_PROMPT = """
Create a concise 2-3 sentence summary of this biotech press release that an investor would find immediately useful. 

Include in your summary:
1. The main announcement with company name and ticker
2. The most significant data point or business impact with exact numbers
3. The next key milestone with specific timeline if mentioned

Use precise terminology, exact percentages/values, and maintain scientific accuracy. Be direct and concise without sacrificing critical details.

Format the first line to include the release date as: [YYYY-MM-DD] Summary text...
"""

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

def load_data_sources(base_path, msg_id):
    """
    Load all data sources for a given message ID.

    :param base_path: Base path for data files
    :param msg_id: Message ID to process
    :return: Dictionary with loaded data sources
    """
    try:
        # Load all data sources
        l0 = read_json(f"{base_path}/l0/{msg_id}/ts.json")
        l1 = read_json(f"{base_path}/l1/{msg_id}/email.json")
        l2 = read_json(f"{base_path}/l2/{msg_id}/selected_link.json")
        main_text = read_file(f"{base_path}/l3/{msg_id}/main_text.txt")
        pdf_summary = read_json(f"{base_path}/l3/{msg_id}/pdfs/pdf_summary.json")
        
        return {
            "l0": l0,
            "l1": l1,
            "l2": l2,
            "main_text": main_text,
            "pdf_summary": pdf_summary
        }
    except Exception as e:
        logger.error(f"Error loading data sources for {msg_id}: {e}")
        raise

def extract_metadata(data_sources):
    """
    Extract metadata from data sources.
    
    :return: Dictionary with extracted metadata
    """
    l0 = data_sources["l0"]
    l1 = data_sources["l1"]
    l2 = data_sources["l2"]
    
    return {
        "retrieved_timestamp": l0.get("retrieved_timestamp", "Unknown"),
        "email_subject": l1.get("subject", "Unknown"),
        "email_body": l1.get("text_body", "Unknown"),
        "email_delivery_time": l1.get("date", "Unknown"),
        "email_sender": l1.get("sender", "Unknown"),
        "link_to_news_release": l2.get("selected_link", "Unknown"),
        "link_selection_method": l2.get("selection_method", "Unknown"),
        "all_available_links": l2.get("all_candidates", []),
        "main_content": data_sources["main_text"]
    }

def filter_pdfs(pdf_summary, max_pdfs=3, max_pages=5):
    """
    Filter PDFs based on basic criteria.
    
    :param pdf_summary: List of PDF summaries
    :param max_pdfs: Maximum number of PDFs to include
    :param max_pages: Maximum number of pages allowed
    :return: Filtered list of PDFs
    """
    if not pdf_summary:
        return []
        
    filtered_pdfs = []
    for pdf in pdf_summary:
        if pdf.get("error") is not None or not pdf.get("text"):
            continue
        if pdf.get("num_pages", 0) > max_pages:
            continue
        filtered_pdfs.append(pdf)
    
    return filtered_pdfs[:max_pdfs] if len(filtered_pdfs) > max_pdfs else filtered_pdfs

def format_pdf_content(filtered_pdfs):
    """
    Format PDF content for inclusion in prompt.
    
    :param filtered_pdfs: List of filtered PDFs
    :return: Formatted string of PDF content
    """
    return "\n\n".join([
        f"""--- PDF: {pdf['filename']} ---
URL: {pdf['url']}
{pdf['text']}""" for pdf in filtered_pdfs
    ])

def combine_content(metadata, pdf_content):
    """
    Combine metadata and PDF content into a single string.

    :param metadata: Dictionary with extracted metadata
    :param pdf_content: Formatted string of PDF content
    :return: Combined content string
    """
    return f"Subject: {metadata['email_subject']}\n\n" \
           f"Email Body:\n{metadata['email_body']}\n\n" \
           f"Press Release Page:\n{metadata['main_content']}\n\n" \
           f"PDF Attachments:\n{pdf_content}"

def chunk_text(text, tokenizer, max_tokens=100000):
    """
    Chunk text based on accurate token counts using tiktoken.
    
    :text: Text to chunk
    :tokenizer: Tokenizer for encoding
    :max_tokens: Maximum tokens per chunk
    :return: List of text chunks
    """
    tokens = tokenizer.encode(text)

    if len(tokens) <= max_tokens:
        return [text]

    chunks = []
    paragraphs = text.split("\n\n")
    
    current_chunk_tokens = []
    current_paragraphs = []
    
    for paragraph in paragraphs:
        paragraph_tokens = tokenizer.encode(paragraph)
        if current_chunk_tokens and len(current_chunk_tokens) + len(paragraph_tokens) > max_tokens:
            chunked_text = "\n\n".join(current_paragraphs)
            chunks.append(chunked_text)
            current_chunk_tokens = paragraph_tokens
            current_paragraphs = [paragraph]
        else:
            current_chunk_tokens.extend(paragraph_tokens)
            current_paragraphs.append(paragraph)
    
    if current_paragraphs:
        chunks.append("\n\n".join(current_paragraphs))
    
    return chunks

def generate_llm_summaries(chat, chunks, system_prompt, short_prompt):
    """
    Generate LLM summaries from content chunks.
    
    :param chat: Chat object for LLM interaction
    :param chunks: List of text chunks
    :param system_prompt: System prompt for LLM
    :param short_prompt: Short prompt for LLM
    :return: Dictionary with full and email summaries
    """
    logger.info(f"Processing {len(chunks)} content chunks")
    
    summaries = [chat.ask(system_prompt, chunk) for chunk in chunks]
    full_summary = "\n\n".join(summaries)
    
    email_summary = chat.ask(short_prompt, full_summary)
    
    return {
        "full_summary": full_summary,
        "email_summary": email_summary
    }

def extract_timestamp_from_summary(summary):
    """
    Extract the formatted timestamp from the summary.
    
    :param summary: Summary text
    :return: Formatted timestamp string
    """
    timestamp_match = re.search(r"RELEASE TIMESTAMP:(.*?)(?:\n|$)", summary)
    if timestamp_match:
        return timestamp_match.group(1).strip()
    return "Unknown"

def save_outputs(base_path, msg_id, metadata, summaries, release_timestamp, pdf_stats):
    """
    Save all outputs to disk.
    
    :param base_path: Base path for data files
    :param msg_id: Message ID to process
    :param metadata: Dictionary with extracted metadata
    :param summaries: Dictionary with generated summaries
    :param release_timestamp: Formatted release timestamp
    :param pdf_stats: Dictionary with PDF statistics
    :return: None
    """
    out_dir = f"{base_path}/l4/{msg_id}"
    os.makedirs(out_dir, exist_ok=True)
    
    summary_ts = datetime.now(eastern_tz).strftime("%Y-%m-%d %H:%M:%S %Z")
    
    final_json = {
        "release_timestamp": release_timestamp,
        "email_delivery_time": convert_to_eastern(metadata["email_delivery_time"]),
        "retrieved_timestamp": convert_to_eastern(metadata["retrieved_timestamp"]),
        "summary_ts": summary_ts,
        "email_sender": metadata["email_sender"],
        "email_subject": metadata["email_subject"],
        "email_body": metadata["email_body"],
        "link_to_news_release_from_email": metadata["link_to_news_release"],
        "link_selection_method_from_email": metadata["link_selection_method"],
        "all_available_links_from_email": metadata["all_available_links"],
        "main_content_from_news_release_page": metadata["main_content"],
        "pdf_count": pdf_stats["total"],
        "analyzed_pdf_count": pdf_stats["analyzed"],
        "page_summary": summaries["full_summary"],
        "email_summary": summaries["email_summary"]
    }

    with open(f"{out_dir}/page_summary.txt", "w") as f:
        f.write(summaries["full_summary"])
    with open(f"{out_dir}/email_summary.txt", "w") as f:
        f.write(summaries["email_summary"])
    with open(f"{out_dir}/final_output.json", "w") as f:
        json.dump(final_json, f, indent=2)
    
    logger.info(f"Stored summary files for {msg_id}")

def summarize_release(**context):
    """
    Main function to summarize press releases.
    Orchestrates the atomic functions in the proper sequence.
    
    :param context: Airflow context
    :return: None
    """

    extraction_summary = context["params"].get("extraction_summary", {})
    chat = Chat(ChatConfig(model="gpt-4o", temperature=0, max_tokens=1024))
    tokenizer = tiktoken.encoding_for_model("gpt-4o")
    
    successful_count = 0
    failed_count = 0
    successful_entries = []

    for msg_id in extraction_summary:
        try:
            logger.info(f"Summarizing release for: {msg_id}")
            base_path = f"{DATA_PATH}"
            data_sources = load_data_sources(base_path, msg_id)
            metadata = extract_metadata(data_sources)
            filtered_pdfs = filter_pdfs(data_sources["pdf_summary"])
            pdf_content = format_pdf_content(filtered_pdfs)
            pdf_stats = {
                "total": len(data_sources["pdf_summary"]) if data_sources["pdf_summary"] else 0,
                "analyzed": len(filtered_pdfs)
            }
            combined_content = combine_content(metadata, pdf_content)
            chunks = chunk_text(combined_content, tokenizer)
            summaries = generate_llm_summaries(chat, chunks, SYSTEM_PROMPT, SHORT_SYSTEM_PROMPT)
            release_timestamp = extract_timestamp_from_summary(summaries["full_summary"])
            save_outputs(base_path, msg_id, metadata, summaries, release_timestamp, pdf_stats)
            
            # Add to successful entries
            successful_entries.append({
                "msg_id": msg_id,
                "release_timestamp": release_timestamp,
                "email_summary": summaries["email_summary"]
            })
            successful_count += 1

        except Exception as e:
            logger.error(f"Failed summarizing {msg_id}: {e}")

            failed_count += 1

    # Push results to XCom
    context["ti"].xcom_push(key="successful_entries", value=successful_entries)
    logging.info(f"Summary completed: {successful_count} successful, {failed_count} failed")


def publish_metrics_to_postgres(**context):
    """
    Publish metrics to PostgreSQL database.
    
    :param context: Airflow context
    :return: None
    """
    try:
        successful_entries = context["ti"].xcom_pull(task_ids="summarize_release", key="successful_entries")
        logger.info(f"Publishing metrics: {len(successful_entries)} successful entries")
        if not successful_entries:
            logger.warning("No successful entries to publish.")
            return
        
        db_service = DBService()

        for entry in successful_entries:
            db_service.publish_summary_metric(entry)
            logger.info(f"Published summary metric for {entry['msg_id']}")

        logger.info("Metrics published to PostgreSQL database")
    except Exception as e:
        logger.error(f"Failed to publish metrics to PostgreSQL: {e}")
        raise

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

    publish_metrics = PythonOperator(
        task_id="publish_metrics",
        python_callable=publish_metrics_to_postgres,
        provide_context=True
    )

    end = EmptyOperator(task_id="end")

    start >> summarize >> publish_metrics >> end
