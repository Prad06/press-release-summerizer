# Press Release Summarizer

A modular, low-latency pipeline to automatically monitor biotech company press releases, extract the main content from emails and linked documents, summarize it using an LLM, and store the structured output for downstream usage.

---

## Architecture Overview

![Architecture Overview](arch.png)

Each stage is modular and fault-tolerant with clean separation of concerns, enabling independent DAG retries, logging, and expansion.

---

## Watcher Service (Pub/Sub Listener)

- Lightweight Python service that listens for Gmail push notifications via **Google Cloud Pub/Sub**.
- Automatically triggers the **L1 ingestion DAG** when a new email arrives in the subscribed inbox.
- Uses Gmail’s `historyId` to ensure only new messages are processed.

### Features

- **Low-latency execution** — near-instantaneous trigger on email receipt.
- **Persistent `historyId` tracking** to avoid duplicate processing (stored in the database).
- **Token management** — OAuth2 credentials handled securely via config paths or Secret Manager.
- **Fault-tolerant** — logs and gracefully handles malformed Pub/Sub messages.
- **Modular & reusable** — supports scaling across multiple Gmail accounts or Pub/Sub topics.

---

## Email Ingestion (L1)

- Subscribed to 20+ biotech IR mailing lists.
- Uses Gmail API to receive emails in near real-time via Pub/Sub.
- Raw `.eml` content is saved to disk along with extracted metadata (sender, subject, body, etc.).

---

## Link Extraction (L2)

- Filters hyperlinks in HTML emails using keyword-based heuristics.
- Discards unsubscribe, social, and irrelevant links.
- Saves top candidate links for scraping.
- Supports edge cases where emails embed full press releases.

---

## Content Scraping (L3)

- Uses **Trafilatura** for HTML content extraction from IR websites.
- Falls back to **Selenium** for dynamic pages or broken DOMs.
- Parses PDF links (via PyPDF2) and extracts full text.
- Normalizes all URLs (including `//domain.com/...`) using `urljoin` logic.

---

## LLM Summarization (L4)

- Uses OpenAI GPT-4 via `ChatConfig` for structured summarization.
- **Inputs**:
    - Email subject and body
    - Extracted web page content
    - Analyzed PDF content
- **Outputs**:
    - **Email Summary**
    - **Main Page Summary**
    - Timestamps for each stage

---

## Final Output Format

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

## Deployment

### Stage 1: Provision the GCP VM

> Workflow: `.github/workflows/provision-vm.yml`

#### GitHub Secrets

- **GCP_SA_KEY** — Roles required:
    ```
    - roles/compute.admin
    - roles/compute.securityAdmin
    - roles/iam.serviceAccountUser
    ```

- **WATCHER_ENV**:
    ```
    # Database Configuration
    DB_NAME=kcap_db
    DB_USER=your-db-user-name
    DB_PASSWORD=your-db-user-password
    DB_HOST=your-db-ip
    DB_PORT=5432

    # Google Authentication
    GOOGLE_APPLICATION_CREDENTIALS=./config/google_sa.json
    GMAIL_OAUTH2_CREDENTIALS=./config/credentials.json
    GMAIL_USER_EMAIL=email-attached-to-gmail-api
    PUBSUB_USER_EMAIL=email-attached-to-gmail-api
    GOOGLE_CLOUD_PROJECT=your-google-cloud-project

    # PubSub Configuration
    PUBSUB_TOPIC_NAME=your-pubsub-topic
    PUBSUB_SUBSCRIPTION_NAME=your-pubsub-subscription

    # Email Monitoring
    TARGET_EMAIL=your-target-email

    # Airflow Configuration
    AIRFLOW_API_URL=http://VM_EXTERNAL_IP:8080/api/v1
    AIRFLOW_USERNAME=your-airflow-username
    AIRFLOW_PASSWORD=your-airflow-password
    PROCESS_GMAIL_DAG_ID=gmail_download_and_parse_l1

    # OpenAI API Key
    OPENAI_API_KEY=your-open-ai-key
    ```

- **AIRFLOW_ENV**:
    ```
    # Database Configuration
    DB_NAME=kcap_db
    DB_USER=your-db-user-name
    DB_PASSWORD=your-db-user-password
    DB_HOST=your-db-ip
    DB_PORT=5432

    # Google Authentication
    GOOGLE_APPLICATION_CREDENTIALS=/opt/airflow/config/google_sa.json
    GMAIL_OAUTH2_CREDENTIALS=/opt/airflow/config/credentials.json
    GMAIL_USER_EMAIL=email-attached-to-gmail-api
    PUBSUB_USER_EMAIL=email-attached-to-gmail-api
    GOOGLE_CLOUD_PROJECT=your-google-cloud-project

    # PubSub Configuration
    PUBSUB_TOPIC_NAME=your-pubsub-topic
    PUBSUB_SUBSCRIPTION_NAME=your-pubsub-subscription

    # Email Monitoring
    TARGET_EMAIL=your-target-email

    # Airflow Configuration
    AIRFLOW_API_URL=http://VM_EXTERNAL_IP:8080/api/v1
    AIRFLOW_USERNAME=your-airflow-username
    AIRFLOW_PASSWORD=your-airflow-password
    PROCESS_GMAIL_DAG_ID=gmail_download_and_parse_l1

    # Airflow Web UI Credentials
    _AIRFLOW_WWW_USER_USERNAME=your-airflow-username
    _AIRFLOW_WWW_USER_PASSWORD=your-airflow-password

    # Project directory
    AIRFLOW_PROJ_DIR=.

    # File ownership
    AIRFLOW_UID=1000
    AIRFLOW_GID=0

    # DAGs, Plugins, Logs
    AIRFLOW_DAGS_DIR=./dags
    AIRFLOW_PLUGINS_DIR=./plugins
    AIRFLOW_LOGS_DIR=./logs

    # SQLAlchemy connection string
    AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:airflow@postgres/airflow

    # Celery result backend (PostgreSQL)
    AIRFLOW__CELERY__RESULT_BACKEND=db+postgresql://airflow:airflow@postgres/airflow

    # Celery broker (Redis)
    AIRFLOW__CELERY__BROKER_URL=redis://redis:6379/0

    # Logging configuration
    AIRFLOW__LOG__WORKER_LOG_SERVER_PORT=8793

    # Postgres credentials
    POSTGRES_USER=airflow
    POSTGRES_PASSWORD=airflow
    POSTGRES_DB=airflow

    # OpenAI API Key
    OPENAI_API_KEY=your-open-ai-key

    # ChromeDriver path
    CHROME_BIN=/usr/bin/google-chrome
    CHROMEDRIVER_BIN=/usr/bin/chromedriver

    # Data Path
    DATA_PATH=/opt/airflow/tmp
    ```

- **GOOGLE_CREDENTIALS**:
    ```
    Enable the Gmail VM and download the OAuth2 Credentials.
    ```

Update the project ID and other details in the deploy script and action.

### Stage 2: Set up actions runner on the GCP VM

- SSH into the configured VM.
- Follow the instructions on this page: `https://github.com/{your-username}/press-release-summerizer/settings/actions/runners/new?arch=x64&os=linux`.

### Stage 3: Deploy the Service

> Workflow: `.github/workflows/deploy-on-vm.yml`

Run this workflow to deploy Airflow and Watcher Service.

### Stage 4: Validate the deployment

- SSH into the VM and run `docker ps`, check for the health of all the containers.
- Visit `<YOUR-EXTERNAL-IP:8080>` to access the Airflow UI.

---

## Latency & Edge Case Handling

- Designed to trigger summarization within **30–60 seconds** after email arrival (depending on scraping load).
- **Gmail → Pub/Sub Triggering**: The watcher service uses Gmail's Pub/Sub notifications to asynchronously trigger the pipeline as soon as a new email arrives, eliminating polling and reducing response time to under a few seconds.
- **Parallel DAG Design**: The pipeline is divided into modular Airflow DAGs (L1–L4), allowing independent execution and parallel processing of message IDs.
- **Efficient I/O**: Intermediate files are cached locally in-memory or to disk only where necessary (e.g., raw HTML, PDFs) to minimize I/O bottlenecks.
- **Selective LLM Inputs**: Only essential content (email plain text, subject, page body) is passed to the LLM, avoiding unnecessary tokenization costs and reducing inference latency.
- Handles:
    - Broken or relative PDF/HTML URLs.
    - Non-parsable PDFs (skips with logs).
    - HTML-only or plaintext-only emails.
    - Missing headers (defaults to empty strings with warnings).

---

## Strategy for Scaling to More Companies

1. **Custom Company-Specific Scrapers**:
     - For better accuracy and adaptability, the pipeline is designed to support **dedicated scrapers per company**, especially for IR sites with complex or inconsistent layouts.
     - These scrapers will leverage:
         - **BeautifulSoup** for structured tag extraction (e.g., headlines, timestamps).
         - **Trafilatura** for full-text extraction when structure is uniform.
         - **Selenium** for JavaScript-heavy or dynamically rendered pages.
     - Each scraper module will follow a consistent interface and be registered by domain, enabling easy expansion.

2. **LLM-based PDF Ranking**:
     - When multiple PDFs are attached or linked (e.g., earnings reports, investor presentations), the system will use an **LLM-based scoring heuristic** to rank and select the most relevant document.
     - The LLM can evaluate metadata and first-page content to determine likelihood of relevance (e.g., presence of keywords like "Press Release", "Clinical Trial", or "FDA Approval").
     - This approach ensures the **most informative and contextually relevant PDF** is selected for summarization when multiple options exist.

Together, these strategies make the system scalable and intelligent—capable of adapting to a growing and diverse set of IR sources while maintaining high accuracy and low latency.

---

## Folder Structure

```
Directory structure:
└── prad06-press-release-summerizer/
    ├── README.md
    ├── deploy.sh
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
    ├── src/
    │   ├── chat/
    │   │   ├── __init__.py
    │   │   ├── chat.py
    │   │   └── config.py
    │   ├── parsers/
    │   │   ├── __init__.py
    │   │   └── email.py
    │   └── services/
    │       ├── __init__.py
    │       ├── db/
    │       │   ├── __init__.py
    │       │   ├── base.py
    │       │   └── models.py
    │       ├── google/
    │       │   ├── __init__.py
    │       │   ├── auth.py
    │       │   ├── gmail.py
    │       │   └── pubsub.py
    │       ├── trigger/
    │       │   ├── __init__.py
    │       │   └── trigger.py
    │       └── watcher/
    │           ├── __init__.py
    │           └── watcher.py
    └── .github/
        └── workflows/
            ├── deploy-on-vm.yml
            └── provision-vm.yml

```
