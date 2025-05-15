from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from datetime import datetime
import os
import sys
import json
import logging

sys.path.append("/opt/src")

from chat import Chat, ChatConfig

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

DATA_PATH = os.environ.get("DATA_PATH")


def filter_candidate_links(**context):
    """
    Parse L1 email JSONs to extract candidate press release links.

    - Filters out non-content URLs (e.g., social, mailto, login).
    - Scores filtered links using heuristics (e.g., URL patterns, subject keywords).
    - Pushes filtered results to XCom with:
        - filtered_links
        - heuristic_best_link
        - all_scored
        - email_subject and text
    """
    message_ids = context["params"].get("message_ids", [])
    filtered_results = {}

    blacklist_keywords = [
        "mailto:", "unsubscribe", "privacy", "contact", "about", 
        "facebook.com", "linkedin.com", "instagram.com",
        "youtube.com", "#", "login", "signin", "terms", "legal"
    ]

    press_patterns = [
        "/schedule", "/news-release-details", "/news-details", "/pressreleases",
        "/press-release", "/newsroom", "/investors", "/investor-relations",
    ]

    logger.info(f"Starting candidate link filtering for message IDs: {message_ids}")

    for msg_id in message_ids:
        try:
            input_path = f"{DATA_PATH}/l1/{msg_id}/email.json"
            with open(input_path, "r") as f:
                email_json = json.load(f)
                links = email_json.get("links", [])
                subject = email_json.get("subject", "")

            if not links:
                logger.warning(f"[{msg_id}] No links found in email")
                continue

            filtered_links = []
            for link in links:
                if not link or not isinstance(link, str):
                    continue
                if any(keyword in link.lower() for keyword in blacklist_keywords):
                    continue
                filtered_links.append(link)

            scored_links = []
            for link in filtered_links:
                score = 0
                for pattern in press_patterns:
                    if pattern in link.lower():
                        score += 3
                if any(char.isdigit() for char in link):
                    score += 1
                if "?" in link and len(link.split("?")[1]) > 20:
                    score -= 2
                if "?" not in link:
                    score += 1
                if subject:
                    words = [w.lower() for w in subject.split() if len(w) > 3]
                    for word in words:
                        if word in link.lower():
                            score += 1
                            break
                scored_links.append({"url": link, "score": score})

            scored_links.sort(key=lambda x: (x["score"], len(x["url"])), reverse=True)
            best_heuristic_link = scored_links[0]["url"] if scored_links else None

            filtered_results[msg_id] = {
                "filtered_links": filtered_links,
                "heuristic_best_link": best_heuristic_link,
                "all_scored": scored_links,
                "subject": subject,
                "email_subject": subject,
                "email_text": email_json.get("text_body", "")
            }

            logger.info(f"[{msg_id}] Filtered {len(filtered_links)} of {len(links)} links")
            logger.info(f"[{msg_id}] Heuristic top link: {best_heuristic_link}")

        except Exception as e:
            logger.error(f"[{msg_id}] Failed during link filtering: {e}")

    context["ti"].xcom_push(key="filtered_results", value=filtered_results)


def select_best_link_llm(**context):
    """
    Use an LLM to select the most likely press release link from filtered links.

    - Uses GPT to choose best match if multiple links.
    - Falls back to heuristics or first link if LLM fails.
    - Saves:
        - selected_link
        - llm_response
        - selection_method
    """
    config = ChatConfig(
        model="gpt-3.5-turbo",
        temperature=0,
        max_tokens=1024
    )
    chat = Chat(config)

    filtered_results = context["ti"].xcom_pull(
        key="filtered_results", task_ids="filter_candidate_links"
    )

    if not filtered_results:
        logger.warning("No filtered results found from previous task.")
        return

    final_results = {}
    successful_message_links = {}

    for msg_id, data in filtered_results.items():
        try:
            filtered_links = data.get("filtered_links", [])
            email_subject = data.get("email_subject", "")
            email_text = data.get("email_text", "")
            email_snippet = email_text[:300] + ("..." if len(email_text) > 300 else "")

            if not filtered_links:
                logger.warning(f"[{msg_id}] No filtered links to evaluate")
                continue

            if len(filtered_links) == 1:
                selected_link = filtered_links[0]
                method = "only_link"
                llm_response = None
            else:
                system_msg = """You are a highly accurate assistant trained to identify press release URLs from investor-related biotech emails.

A valid press release link typically meets the following criteria:
1. It resides on the official website of the company.
2. It includes keywords like 'press', 'news', 'release', or 'announcement' in the URL path.
3. It points directly to a specific article or statement, not a homepage, contact page, or social media.
4. It avoids redirect or tracking-heavy URLs, login pages, and generic news aggregators.

Only select the single most relevant link that likely leads to the detailed press release mentioned in the email."""

                user_msg = f"""Given this biotech company email:
Subject: {email_subject}
Email snippet: {email_snippet}

Which ONE of these URLs most likely points to the press release mentioned in the email?

Links:
{json.dumps(filtered_links, indent=2)}

Return ONLY the full URL of the most likely press release link, with no explanation.
If none of these links appear to be press releases, respond with exactly "NO_PRESS_RELEASE_FOUND".
"""

                try:
                    response = chat.ask(system_msg, user_msg).strip()
                    llm_response = {
                        "system_message": system_msg,
                        "user_message": user_msg,
                        "llm_response": response
                    }

                    if response == "NO_PRESS_RELEASE_FOUND":
                        selected_link = filtered_links[0]
                        method = "llm_no_match"
                    elif response in filtered_links:
                        selected_link = response
                        method = "llm"
                    else:
                        selected_link = filtered_links[0]
                        method = "unexpected_response"

                except Exception as e:
                    logger.error(f"[{msg_id}] LLM selection failed: {e}")
                    selected_link = filtered_links[0]
                    method = "error_fallback"
                    llm_response = {"error": str(e)}

            final_results[msg_id] = {
                "selected_link": selected_link,
                "selection_method": method,
                "all_candidates": filtered_links,
                "llm_response": llm_response,
                "heuristic_data": {
                    "best_link": data.get("heuristic_best_link"),
                    "scored_links": data.get("all_scored")
                }
            }

            successful_message_links[msg_id] = selected_link
            logger.info(f"[{msg_id}] Selected link: {selected_link} (method: {method})")

        except Exception as e:
            logger.error(f"[{msg_id}] Error during link selection: {e}")

    context["ti"].xcom_push(key="final_results", value=final_results)
    context["ti"].xcom_push(key="successful_message_links", value=successful_message_links)
    logger.info(f"Total selected links: {len(successful_message_links)}")


def write_results_to_disk(**context):
    """
    Write selected link results and LLM debug info to disk.

    - Output folder: {DATA_PATH}/l2/<message_id>/
    - Files:
        - selected_link.json
        - debug_info.json
    """
    final_results = context["ti"].xcom_pull(
        key="final_results", task_ids="select_best_link_llm"
    )

    if not final_results:
        logger.warning("No final results to write.")
        return

    for msg_id, data in final_results.items():
        try:
            l2_dir = f"{DATA_PATH}/l2/{msg_id}"
            os.makedirs(l2_dir, exist_ok=True)

            selected_link_path = f"{l2_dir}/selected_link.json"
            with open(selected_link_path, "w") as f:
                json.dump({
                    "selected_link": data["selected_link"],
                    "selection_method": data["selection_method"],
                    "all_candidates": data["all_candidates"]
                }, f, indent=2)

            debug_path = f"{l2_dir}/debug_info.json"
            with open(debug_path, "w") as f:
                json.dump(data, f, indent=2)

            logger.info(f"[{msg_id}] Wrote selected_link and debug_info to disk")

        except Exception as e:
            logger.error(f"[{msg_id}] Failed to write results to disk: {e}")


def trigger_l3_dag(**context):
    """
    Trigger L3 DAG: extract_release_information_l3 using selected links.

    - XCom input: successful_message_links
    - Config: {message_links: {msg_id: selected_link}}
    """
    logger.info("Starting trigger_l3_dag")

    try:
        successful_message_links = context["ti"].xcom_pull(
            key="successful_message_links", task_ids="select_best_link_llm"
        )

        if not successful_message_links:
            logger.warning("No message links to trigger L3 DAG.")
            return

        conf = {"message_links": successful_message_links}

        trigger_task = TriggerDagRunOperator(
            task_id="trigger_extract_release_information_l3",
            trigger_dag_id="extract_release_information_l3",
            conf=conf,
            reset_dag_run=True,
            wait_for_completion=False,
        )

        trigger_task.execute(context=context)

        logger.info(f"L3 DAG triggered with message IDs: {list(successful_message_links.keys())}")

    except Exception as e:
        logger.error(f"Failed to trigger L3 DAG: {e}")
        raise

    finally:
        logger.info("Finished trigger_l3_dag")


# Define DAG
with DAG(
    dag_id="find_news_release_link_l2",
    description="Pipeline to identify the most likely press release link from parsed biotech emails",
    start_date=datetime(2025, 5, 12),
    schedule_interval=None,
    catchup=False,
    tags=["gmail", "press-release", "link"],
    params={"message_ids": []},
) as dag:

    start = EmptyOperator(task_id="start")

    filter_links = PythonOperator(
        task_id="filter_candidate_links",
        python_callable=filter_candidate_links,
        provide_context=True,
    )

    select_link = PythonOperator(
        task_id="select_best_link_llm",
        python_callable=select_best_link_llm,
        provide_context=True,
    )

    write_results = PythonOperator(
        task_id="write_results_to_disk",
        python_callable=write_results_to_disk,
        provide_context=True,
    )

    trigger_l3_task = PythonOperator(
        task_id="trigger_l3_dag",
        python_callable=trigger_l3_dag,
        provide_context=True,
    )

    end = EmptyOperator(task_id="end")

    start >> filter_links >> select_link >> write_results >> trigger_l3_task >> end
