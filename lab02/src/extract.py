import requests
import logging
from datetime import datetime, timezone
from time import sleep


logger = logging.getLogger(__name__)
BASE_URL = "https://jsonplaceholder.typicode.com"

def fetch_with_retry(url: str, max_retries: int = 3) -> list[dict]:
    for attempt in range(max_retries):
        try:
            resp = requests.get(url, timeout=30)
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as e:
            if attempt == max_retries - 1:
                raise
            wait = 2 ** attempt
            logger.warning(f"Attempt {attempt+1} failed: {e}. Retrying in {wait}s...")
            sleep(wait)


def add_metadata(records: list[dict]) -> list[dict]:
    ts = datetime.now(timezone.utc).isoformat()
    for r in records:
        r["ingested_at"] = ts
    return records
    
def extract_users() -> list[dict]:
    data = fetch_with_retry(f"{BASE_URL}/users")
    logger.info(f"Users: {len(data)} records fetched")
    return add_metadata(data)
   


def extract_posts() -> list[dict]:
    data = fetch_with_retry(f"{BASE_URL}/posts")
    logger.info(f"Posts: {len(data)} records fetched")
    return add_metadata(data)
 

def extract_comments() -> list[dict]:
    data = fetch_with_retry(f"{BASE_URL}/comments")
    logger.info(f"Comments: {len(data)} records fetched")
    return add_metadata(data)
   