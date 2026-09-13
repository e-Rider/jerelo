import json
import logging
import os
import re
import time
from datetime import datetime
import boto3
from botocore.exceptions import ClientError, NoCredentialsError
import requests

# Set up logging configuration
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

# API Configuration Constants
HOST = "https://api.openprocurement.org"
TENDERS_ENDPOINT = f"{HOST}/api/2.5/tenders"

# Compiled regular expression for pharmaceutical CPV codes (33600000-6 range)
CPV_PATTERN = re.compile(r"^336\d{2}000-\d$")

# Dynamic filename generation using current UTC date
TODAY_STR = datetime.now().strftime("%Y_%m_%d")
STORE_PATH = f"raw_tenders_{TODAY_STR}.jsonl"


def append_tender_to_jsonl(tender: dict, file_path: str = STORE_PATH) -> None:
    """Appends a single tender object to a JSON Lines file.

    Args:
        tender (dict): Tender payload object.
        file_path (str): Target file path in JSON Lines format.
    """
    with open(file_path, "a", encoding="utf-8") as f:
        f.write(json.dumps(tender, ensure_ascii=False) + "\n")


def fetch_tender_details(session: requests.Session, tender_id: str) -> dict | None:
    """Fetch full details for a single tender by its unique ID.

    Args:
        session (requests.Session): Active HTTP session for connection pooling.
        tender_id (str): Unique Prozorro tender ID.

    Returns:
        dict | None: Tender data payload if request succeeds, None otherwise.
    """
    url = f"{TENDERS_ENDPOINT}/{tender_id}"
    try:
        response = session.get(url, timeout=10)
        if response.status_code == 200:
            return response.json()
        logging.error(f"Failed to fetch tender '{tender_id}': HTTP {response.status_code}")
    except requests.RequestException as error:
        logging.error(f"Network error while fetching tender '{tender_id}': {error}")
    return None


def extract_matching_cpv(tender_data: dict, cpv_pattern: re.Pattern) -> str:
    """Search for the first item CPV code matching the regular expression pattern.

    Args:
        tender_data (dict): Complete tender payload.
        cpv_pattern (re.Pattern): Regular expression pattern for the CPV code.

    Returns:
        str: Matched CPV code string if found, otherwise an empty string.
    """
    tender_items = tender_data.get("data", {}).get("items", [])
    for item in tender_items:
        item_cpv = item.get("classification", {}).get("id", "")
        if cpv_pattern.match(item_cpv):
            return item_cpv
    return ""


def crawl_prozorro_tenders(max_pages: int = 10, 
                            page_size: int = 100, 
                            cpv_pattern: re.Pattern = CPV_PATTERN
                            ) -> int:
    """Paginate through Prozorro tenders feed and filter tenders matching 
        medical CPV codes.

    Args:
        max_pages (int): Maximum number of pagination pages to process.
        page_size (int): Number of tenders per page requested from API.
        cpv_pattern (re.Pattern): Regex pattern to filter medical items.

    Returns:
        int: Count of filtered tender payloads containing matching CPV codes.
    """
    current_page = 1
    next_page_url = f"{TENDERS_ENDPOINT}?descending=1&limit={page_size}"
    medical_tenders = 0

    # Use HTTP Session for re-using TCP connections across requests
    with requests.Session() as session:
        while current_page <= max_pages and next_page_url:
            logging.info(f"Processing page {current_page}/{max_pages}: {next_page_url}")
            try:
                response = session.get(next_page_url, timeout=10)
                if response.status_code != 200:
                    logging.error(f"Failed to retrieve page {current_page}: HTTP {response.status_code}")
                    break

                feed = response.json()
                tenders_list = feed.get("data", [])
                if not tenders_list:
                    logging.info("No more tenders found in feed.")
                    break

                for tender in tenders_list:
                    tender_id = tender.get("id")
                    if not tender_id:
                        continue

                    time.sleep(0.05) # delay to prevent HTTP 429 (Too Many Requests)
                    tender_details = fetch_tender_details(session, tender_id)

                    if tender_details:
                        matched_cpv = extract_matching_cpv(tender_details, cpv_pattern)
                        if matched_cpv:
                            logging.info(f"-> Found matching tender: ID {tender_id} (CPV: {matched_cpv})")
                            append_tender_to_jsonl(tender_details, STORE_PATH)
                            medical_tenders += 1
                        else:
                            logging.info(f"-> Tender ID {tender_id} does not contain matching CPV codes.")

                # Extract URL for the next page in pagination
                next_page_url = feed.get("next_page", {}).get("uri")
                current_page += 1
                time.sleep(0.5)

            except requests.RequestException as error:
                logging.error(f"Network error on page {current_page}: {error}")
                break

    return medical_tenders


def upload_to_s3(
    file_path: str, bucket_name: str, object_name: str = None
) -> bool:
    """Uploads a local file to an AWS S3 bucket using boto3.

    Credentials are automatically retrieved from environment variables:
    AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, and AWS_DEFAULT_REGION.

    Args:
        file_path (str): Path to the local file.
        bucket_name (str): Destination S3 bucket name.
        object_name (str, optional): S3 key path. Defaults to raw/prozorro/<filename>.

    Returns:
        bool: True if upload succeeded, False otherwise.
    """
    if not object_name:
        object_name = f"raw/prozorro/{os.path.basename(file_path)}"

    s3_client = boto3.client("s3")

    try:
        logging.info(
            f"Starting upload for {file_path} to s3://{bucket_name}/{object_name}..."
        )
        s3_client.upload_file(file_path, bucket_name, object_name)
        logging.info("File successfully uploaded to AWS S3 Data Lake!")
        return True
    except FileNotFoundError:
        logging.error(f"Local file {file_path} was not found.")
        return False
    except NoCredentialsError:
        logging.error("AWS credentials not found in environment variables!")
        return False
    except ClientError as e:
        logging.error(f"AWS S3 ClientError encountered: {e}")
        return False


if __name__ == "__main__":
    logging.info("Starting Prozorro medical tenders ETL process...")
    
    # 1. Extraction step
    total_saved = crawl_prozorro_tenders(max_pages=5, page_size=20)
    logging.info(f"Extraction completed. Total tenders saved locally: {total_saved}")

    # 2. AWS S3 Upload step
    s3_bucket = os.getenv(
        "AWS_S3_BUCKET", "prozorro-data-lake-233810108139-eu-central-1-an"
    )

    if os.path.exists(STORE_PATH) and total_saved > 0:
        upload_to_s3(file_path=STORE_PATH, bucket_name=s3_bucket)
    else:
        logging.warning("No new files were generated to upload to S3.")
