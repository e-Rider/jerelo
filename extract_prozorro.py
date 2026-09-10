import json
import re
import time
import requests

# Configuration constants
HOST = "https://api.openprocurement.org"
TENDERS_ENDPOINT = f"{HOST}/api/2.5/tenders"
CPV_PATTERN = r"^336\d{2}000-\d{1}$" #CPV pharmaceuticals products (starts with 336)
STORE_PATH = "raw_tenders.jsonl"



def save_tenders_to_jsonl(tenders: list, file_path: str = STORE_PATH) -> None:
    """Store list of all tenders to JSON Lines format.
 
    Args:
        tenders (list): List of tender objects retrieved from the API.
        filepath (str): Target file path in JSON Lines format.
    """

    with open(file_path, "w", encoding="utf-8") as f:
        for item in tenders:
            f.write(json.dumps(item, ensure_ascii=False) + "\n")
    print(f"{len(tenders)} tenders written to {file_path}")


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
        print(f"Failed to fetch tender '{tender_id}': HTTP {response.status_code}")
    except requests.RequestException as error:
        print(f"Network error while fetching tender '{tender_id}': {error}")
    return None


def extract_matching_cpv(tender_data: dict, cpv_pattern: str) -> str:
    """Search for the first item CPV code matching the regular expression pattern.

    Args:
        tender_data (dict): Complete tender payload.
        cpv_pattern (str): Regular expression pattern for the CPV code.

    Returns:
        str: Matched CPV code string if found, otherwise an empty string.
    """
    tender_items = tender_data.get("data", {}).get("items", [])
    for item in tender_items:
        item_cpv = item.get("classification", {}).get("id", "")
        if re.match(cpv_pattern, item_cpv):
            return item_cpv
    return ""


def crawl_prozorro_tenders(max_pages: int = 10, 
                            page_size: int = 100, 
                            cpv_pattern: str = CPV_PATTERN
                            ) -> list[dict]:
    """Paginate through Prozorro tenders feed and filter tenders matching 
        medical CPV codes.

    Args:
        max_pages (int): Maximum number of pagination pages to process.
        page_size (int): Number of tenders per page requested from API.
        cpv_pattern (str): Regex pattern to filter medical items.

    Returns:
        list[dict]: List of filtered tender payloads containing matching CPV codes.
    """
    current_page = 1
    next_page_url = f"{TENDERS_ENDPOINT}?descending=1&limit={page_size}"
    medical_tenders = []

    # Use HTTP Session for re-using TCP connections across requests
    with requests.Session() as session:
        while current_page <= max_pages and next_page_url:
            print(f"Processing page {current_page}/{max_pages}: {next_page_url}")
            try:
                response = session.get(next_page_url, timeout=10)
                if response.status_code != 200:
                    print(f"Failed to retrieve page {current_page}: HTTP {response.status_code}")
                    break

                feed = response.json()
                tenders_list = feed.get("data", [])
                if not tenders_list:
                    print("No more tenders found in feed.")
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
                            print(f"-> Found matching tender: ID {tender_id} (CPV: {matched_cpv})")
                            medical_tenders.append(tender_details)
                        else:
                            print(f"-> Tender ID {tender_id} does not contain matching CPV codes.")

                # Extract URL for the next page in pagination
                next_page_url = feed.get("next_page", {}).get("uri")
                current_page += 1
                time.sleep(0.5)

            except requests.RequestException as error:
                print(f"Network error on page {current_page}: {error}")
                break

    return medical_tenders


if __name__ == "__main__":
    print("Starting Prozorro medical tenders extractor...")
    
    # Execute ETL Extract step
    extracted_tenders = crawl_prozorro_tenders(max_pages=10, page_size=20)
    
    if extracted_tenders:
        save_tenders_to_jsonl(extracted_tenders, STORE_PATH)
    else:
        print("No medical tenders matching criteria were found.")

