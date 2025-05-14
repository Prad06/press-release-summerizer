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

def filter_candidate_links(**context):
    """
    Load parsed email JSON from /tmp/l1/<message_id>/email.json
    Remove non-content links (mailto, unsubscribe, socials, etc.)
    Save filtered links to /tmp/l2/<message_id>/links.json
    
    This function also applies heuristic scoring for potential fallback use later.
    """
    message_ids = context["params"].get("message_ids", [])
    
    blacklist_keywords = [
        "mailto:", "unsubscribe", "privacy", "contact", "about", 
        "facebook.com", "linkedin.com", "twitter.com", "instagram.com",
        "youtube.com", "#", "login", "signin", "terms", "legal"
    ]
    
    # Common patterns in press release URLs for heuristic scoring
    press_patterns = [
        "/schedule", "/news-release-details", "/news-details", "/pressreleases",
        "/press-release", "/newsroom", "/investors", "/investor-relations",
    ]
    
    for msg_id in message_ids:
        try:
            input_path = f"/tmp/l1/{msg_id}/email.json"
            output_dir = f"/tmp/l2/{msg_id}"
            output_path = f"{output_dir}/links.json"
            heuristic_path = f"{output_dir}/heuristic_scores.json"
            
            os.makedirs(output_dir, exist_ok=True)
            with open(input_path, "r") as f:
                email_json = json.load(f)
                links = email_json.get("links", [])
                subject = email_json.get("subject", "")
            
            if not links:
                logger.warning(f"No links found in email {msg_id}")
                continue
            
            filtered_links = []
            for link in links:
                if not link or not isinstance(link, str):
                    continue

                if any(keyword in link.lower() for keyword in blacklist_keywords):
                    continue
                
                filtered_links.append(link)
            
            with open(output_path, "w") as f:
                json.dump(filtered_links, f, indent=2)
            
            # Apply heuristic scoring for potential fallback use
            scored_links = []
            for link in filtered_links:
                score = 0
                
                # Boost for press release patterns
                for pattern in press_patterns:
                    if pattern in link.lower():
                        score += 3
                
                # Boost for date-like patterns (YYYY/MM/DD or similar)
                if any(char.isdigit() for char in link):
                    score += 1
                
                # Penalize long query parameters (often tracking or session info)
                if "?" in link and len(link.split("?")[1]) > 20:
                    score -= 2
                
                # Boost for clean URLs
                if "?" not in link:
                    score += 1
                
                # Check for keywords from subject
                if subject:
                    words = [w.lower() for w in subject.split() if len(w) > 3]
                    for word in words:
                        if word in link.lower():
                            score += 1
                            break
                
                scored_links.append({"url": link, "score": score})
            
            # Sort by score, and if scores are the same, prefer the longer link
            scored_links.sort(key=lambda x: (x["score"], len(x["url"])), reverse=True)
            
            # Save heuristic scores for potential fallback
            best_heuristic_link = scored_links[0]["url"] if scored_links else None
            
            heuristic_result = {
                "heuristic_best_link": best_heuristic_link,
                "all_scored": scored_links
            }
            
            with open(heuristic_path, "w") as f:
                json.dump(heuristic_result, f, indent=2)
            
            logger.info(f"Filtered links for {msg_id}: {len(filtered_links)} of {len(links)} passed filtering")
            logger.info(f"Heuristic best link for {msg_id}: {best_heuristic_link}")
            
        except Exception as e:
            logger.error(f"Link filtering failed for {msg_id}: {e}")

def select_best_link_llm(**context):
    """
    Use LLM to pick the best link pointing to the press release.
    Save chosen link to /tmp/l3/<message_id>/selected_link.json
    Save LLM response to /tmp/l2/<message_id>/llm_response.json for debugging.
    """
    config = ChatConfig(
        model="gpt-3.5-turbo",
        temperature=0,
        max_tokens=1024
    )
    chat = Chat(config)

    message_ids = context["params"].get("message_ids", [])
    successful_message_links = {}

    for msg_id in message_ids:
        try:
            input_links_path = f"/tmp/l2/{msg_id}/links.json"
            input_email_path = f"/tmp/l1/{msg_id}/email.json"
            output_dir = f"/tmp/l2/{msg_id}"
            output_path = f"{output_dir}/selected_link.json"
            llm_response_path = f"{output_dir}/llm_response.json"

            os.makedirs(output_dir, exist_ok=True)

            with open(input_links_path, "r") as f:
                filtered_links = json.load(f)

            if not filtered_links:
                logger.warning(f"No filtered links found for {msg_id}")
                continue

            with open(input_email_path, "r") as f:
                email_data = json.load(f)
                email_subject = email_data.get("subject", "")
                email_text = email_data.get("text", "")
                email_snippet = email_text[:300] + ("..." if len(email_text) > 300 else "")

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
If none of these links appear to be press releases, respond with exactly \"NO_PRESS_RELEASE_FOUND\".
"""
                try:
                    response = chat.ask(system_msg, user_msg)
                    response = response.strip()

                    # Save LLM response for debugging
                    llm_response = {
                        "system_message": system_msg,
                        "user_message": user_msg,
                        "llm_response": response
                    }
                    with open(llm_response_path, "w") as f:
                        json.dump(llm_response, f, indent=2)

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
                    logger.error(f"LLM selection failed for {msg_id}: {e}")
                    selected_link = filtered_links[0]
                    method = "error_fallback"
                    llm_response = {"error": str(e)}

            result = {
                "selected_link": selected_link,
                "selection_method": method,
                "all_candidates": filtered_links
            }

            with open(output_path, "w") as f:
                json.dump(result, f, indent=2)

            # Add successful message ID and link to the dictionary
            successful_message_links[msg_id] = selected_link

            logger.info(f"Selected link for {msg_id}: {selected_link} (method: {method})")

        except Exception as e:
            logger.error(f"Link selection failed for {msg_id}: {e}")

    # Save successful message links to context for the next task
    context["ti"].xcom_push(key="successful_message_links", value=successful_message_links)
    logger.info(f"Successful message links: {successful_message_links}")


def trigger_l3_dag(**context):
    """Trigger the L3 pipeline using successful_message_links from XCom"""
    logger.info("Starting trigger_l3_dag")

    try:
        # Retrieve successful_message_links from XCom
        successful_message_links = context["ti"].xcom_pull(
            key="successful_message_links", task_ids="select_best_link_llm"
        )

        if not successful_message_links:
            logger.warning("No valid successful_message_links found to trigger L3 DAG.")
            return

        conf = {"message_links": successful_message_links}

        trigger_task = TriggerDagRunOperator(
            task_id="extract_release_information_l3",
            trigger_dag_id="l3_task_dag",
            conf=conf,
            reset_dag_run=True,
            wait_for_completion=False,
        )

        trigger_task.execute(context=context)

    except Exception as e:
        logger.error(f"Error in trigger_l3_dag: {e}")
        raise

    finally:
        logger.info("Finished trigger_l3_dag")

# Define the DAG
with DAG(
    dag_id="find_news_release_link_l2",
    description="Pipeline to find news release links in parsed emails",
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

    trigger_l3_task = PythonOperator(
        task_id="trigger_l3_dag",
        python_callable=trigger_l3_dag,
        provide_context=True,
    )

    end = EmptyOperator(task_id="end")

    start >> filter_links >> select_link >> trigger_l3_task >> end
