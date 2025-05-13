import os
import logging
from dotenv import load_dotenv

from src.services.watcher import WatcherService

# Load environment variables ONCE at entry point
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

if __name__ == "__main__":
    service = WatcherService()
    try:
        service.start()
    except KeyboardInterrupt:
        service.stop()