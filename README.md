# 📣 Press Release Summarizer

A modular, low-latency pipeline to automatically monitor biotech company press releases, extract the main content from emails and linked documents, summarize it using an LLM, and store the structured output for downstream usage.

---

## 🧩 Architecture Overview

```
enter image here
```

Each stage is modular and fault-tolerant with clean separation of concerns, enabling independent DAG retries, logging, and expansion.

---

## 👀 Watcher Service (Pub/Sub Listener)

* Lightweight Python service that listens for Gmail push notifications via **Google Cloud Pub/Sub**.
* Automatically triggers the **L1 ingestion DAG** when a new email arrives in the subscribed inbox.
* Uses Gmail’s `historyId` to ensure only new messages are processed.

### Features

* **Low-latency execution** — near-instantaneous trigger on email receipt.
* **Persistent `historyId` tracking** to avoid duplicate processing (stored in the database).
* **Token management** — OAuth2 credentials handled securely via config paths or Secret Manager.
* **Fault-tolerant** — logs and gracefully handles malformed Pub/Sub messages.
* **Modular & reusable** — supports scaling across multiple Gmail accounts or Pub/Sub topics.

---

## 📨 Email Ingestion (L1)

- Subscribed to 20+ biotech IR mailing lists.
- Uses Gmail API to receive emails in near real-time via Pub/Sub.
- Raw `.eml` content is saved to disk along with extracted metadata (sender, subject, body, etc.).

---

## 🔗 Link Extraction (L2)

- Filters hyperlinks in HTML emails using keyword-based heuristics.
- Discards unsubscribe, social, and irrelevant links.
- Saves top candidate links for scraping.
- Supports edge cases where emails embed full press releases.

---

## 🕸️ Content Scraping (L3)

- Uses **Trafilatura** for HTML content extraction from IR websites.
- Falls back to **Selenium** for dynamic pages or broken DOMs.
- Parses PDF links (via PyPDF2) and extracts full text.
- Normalizes all URLs (including `//domain.com/...`) using `urljoin` logic.

---

## 🧠 LLM Summarization (L4)

- Uses OpenAI GPT-4o via `ChatConfig` for structured summarization.
- Inputs: 
  - Email subject and body
  - Extracted web page content
  - Analyzed PDF content
- Outputs:
  - **Email Summary**
  - **Main Page Summary**
  - Timestamps for each stage

---

## 🧾 Final Output Format

```json
{
  "release_timestamp": "...",
  "email_delivery_time": "...",
  "retrieved_timestamp": "...",
  "summary_ts": "...",
  "email_sender": "...",
  "email_subject": "...",
  "email_body": "...",
  "link_to_news_release_from_email": "...",
  "link_selection_method_from_email": "...",
  "all_available_links_from_email": ["..."],
  "main_content_from_news_release_page": "...",
  "pdf_count": 1,
  "analyzed_pdf_count": 1,
  "page_summary": "...",
  "email_summary": "..."
}
```

---

## ⚙️ Deployment

### Stage 1: Provision the GCP VM

> Workflow: `.github/workflows/provision-vm.yml`

Make sure the GitHub Secrets have a secret GCP_SA_KEY with the following permissions
- roles/compute.admin
- roles/compute.securityAdmin
- roles/iam.serviceAccountUser 

Update the project id and other details in the deploy script and action.

### Stage 2: 

---

## 🕑 Latency & Edge Case Handling

- Designed to trigger summarization within **30–60 seconds** after email arrival (depending on scraping load).
- Handles:
  - Broken or relative PDF/HTML URLs
  - Non-parsable PDFs (skips with logs)
  - HTML-only or plaintext-only emails
  - Missing headers (defaults to empty strings with warnings)
- Airflow DAGs isolate each task for retry and observability.

---

## 📂 Folder Structure

```
Directory structure:
└── prad06-press-release-summerizer/
    ├── README.md
    ├── docker-compose.yml
    ├── Dockerfile
    ├── main.py
    ├── requirements.txt
    ├── airflow/
    │   ├── docker-compose.yml
    │   ├── Dockerfile
    │   ├── requirements.txt
    │   ├── dags/
    │   │   ├── extract_release_information_l3.py
    │   │   ├── find_news_release_link_l2.py
    │   │   ├── gmail_download_and_parse_l1.py
    │   │   └── summarize_press_release_l4.py
    │   └── scripts/
    │       └── install_chromedriver.py
    └── src/
        ├── chat/
        │   ├── __init__.py
        │   ├── chat.py
        │   └── config.py
        ├── parsers/
        │   ├── __init__.py
        │   └── email.py
        └── services/
            ├── __init__.py
            ├── db/
            │   ├── __init__.py
            │   ├── base.py
            │   └── models.py
            ├── google/
            │   ├── __init__.py
            │   ├── auth.py
            │   ├── gmail.py
            │   └── pubsub.py
            ├── trigger/
            │   ├── __init__.py
            │   └── trigger.py
            └── watcher/
                ├── __init__.py
                └── watcher.py

```
