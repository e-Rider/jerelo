"""
extract_prozorro_gemini.py
Prozorro Data Extractor to AWS S3 (Extract Layer)
"""

import asyncio
import json
import logging
import os
import re
import tempfile
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Optional
from urllib.parse import parse_qs, urlencode, urljoin, urlparse
import aiohttp
import boto3
from botocore.exceptions import ClientError


# Logging configuration
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

# API Constants
HOST = "https://api.openprocurement.org"
TENDERS_ENDPOINT = f"{HOST}/api/2.5/tenders" 
STATE_KEY = "state/prozorro_offset.json"

# Specification: regex for pharmaceutical products
MEDICAL_CPV_PATTERN = re.compile(r"^336\d{5}-\d$")

# Load control and fault tolerance
MAX_CONCURRENT_REQUESTS = 30
REQUEST_TIMEOUT_SECONDS = 10
MAX_RETRIES = 5
FLUSH_EVERY_PAGES = 10


def build_feed_url(
    offset: str | None = None,
    limit: int = 100,
    mode: str | None = None,
    opt_fields: list[str] | None = None,
    descending: str | int | None = None,
) -> str:
    """Generates the initial URL for querying the Prozorro feed with optional parameters."""
    params: dict[str, Any] = {"limit": limit}
    
    if offset:
        params["offset"] = offset
        
    if mode:
        params["mode"] = mode
        
    if opt_fields:
        params["opt_fields"] = (
            ",".join(opt_fields) if isinstance(opt_fields, list) else opt_fields
        )
        
    if descending:
        params["descending"] = descending
        
    return f"{TENDERS_ENDPOINT}?{urlencode(params)}"


def utc_now_iso() -> str:
    """Return the current UTC time in ISO 8601 format (ending with Z)."""
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def public_modified_timestamp(tender: dict[str, Any]) -> float:
    """Get the public timestamp of a feed record, falling back to dateModified."""
    value = tender.get("public_modified")
    if isinstance(value, (int, float)):
        return float(value)

    modified = tender.get("dateModified")
    if isinstance(modified, str):
        try:
            parsed = datetime.fromisoformat(modified.replace("Z", "+00:00"))
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            return parsed.timestamp()
        except ValueError:
            pass
    return 0.0


def extract_matching_cpv(
    tender_data: dict[str, Any], cpv_pattern: re.Pattern[str] = MEDICAL_CPV_PATTERN
) -> str:
    """Check for the presence of a pharmaceutical CPV code and return the first one found."""
    items = tender_data.get("data", {}).get("items") or []
    for item in items:
        if not isinstance(item, dict):
            continue
        cpv_code = item.get("classification", {}).get("id", "")
        if isinstance(cpv_code, str) and cpv_pattern.fullmatch(cpv_code):
            return cpv_code
    return ""


def _retry_delay(attempt: int, retry_after: str | None = None) -> float:
    """Calculate the delay based on the Retry-After header or use Exponential Backoff."""
    if retry_after:
        try:
            return max(0.0, float(retry_after))
        except ValueError:
            logging.warning("Ignoring non-numeric Retry-After header: %s", retry_after)
    return min(30.0, 0.5 * (2 ** (attempt - 1)))


async def fetch_tender_details(
    session: aiohttp.ClientSession,
    tender_id: str,
    semaphore: asyncio.Semaphore,
    max_retries: int = MAX_RETRIES,
) -> dict[str, Any] | None:
    """Asynchronously fetch the full tender payload with rate-limit (429) and timeout handling."""
    url = f"{TENDERS_ENDPOINT}/{tender_id}"
    for attempt in range(1, max_retries + 1):
        try:
            async with semaphore:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT_SECONDS)) as response:
                    if response.status == 200:
                        return await response.json()
                    
                    if response.status not in (429, 500, 502, 503, 504):
                        logging.error("Failed to fetch tender %s: HTTP %s", tender_id, response.status)
                        return None
                    
                    delay = _retry_delay(attempt, response.headers.get("Retry-After"))
                    status = response.status
            
            logging.warning(
                "Transient HTTP %s error for %s; retrying in %.2fs (attempt %s/%s)",
                status, tender_id, delay, attempt, max_retries
            )
            if attempt < max_retries:
                await asyncio.sleep(delay)
                
        except (aiohttp.ClientError, asyncio.TimeoutError) as error:
            if attempt == max_retries:
                logging.error("Network error calling %s: %s", tender_id, error)
                return None
            delay = _retry_delay(attempt)
            logging.warning(
                "Network error for %s; retrying in %.2fs (attempt %s/%s): %s",
                tender_id, delay, attempt, max_retries, error
            )
            await asyncio.sleep(delay)
    return None


async def _fetch_feed(
    session: aiohttp.ClientSession, page_uri: str, max_retries: int = MAX_RETRIES
) -> dict[str, Any]:
    """Download a feed page, raising an error after failed attempts."""
    for attempt in range(1, max_retries + 1):
        try:
            async with session.get(page_uri, timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT_SECONDS)) as response:
                if response.status == 200:
                    return await response.json()
                if response.status not in (429, 500, 502, 503, 504):
                    response.raise_for_status()
                delay = _retry_delay(attempt, response.headers.get("Retry-After"))
                status = response.status
                
            logging.warning(
                "Transient HTTP %s error loading feed; retrying in %.2fs (attempt %s/%s)",
                status, delay, attempt, max_retries
            )
            if attempt < max_retries:
                await asyncio.sleep(delay)
                
        except (aiohttp.ClientError, asyncio.TimeoutError):
            if attempt == max_retries:
                raise
            await asyncio.sleep(_retry_delay(attempt))
    raise RuntimeError(f"Feed request failed after {max_retries} attempts: {page_uri}")


@dataclass
class FeedPageResult:
    """Container for a processed feed page and the cursor for the next one."""
    feed_records: list[dict[str, Any]]
    medical_tenders: list[dict[str, Any]]
    next_page_uri: str | None
    next_offset: str | None
    max_public_modified: float


async def process_feed_page(
    session: aiohttp.ClientSession,
    page_uri: str,
    semaphore: asyncio.Semaphore,
    cpv_pattern: re.Pattern[str] = MEDICAL_CPV_PATTERN,
) -> FeedPageResult:
    """Page processing coordinator: collects metadata and parallelizes detail fetching."""
    feed = await _fetch_feed(session, page_uri)
    tenders = feed.get("data") or []
    next_page = feed.get("next_page") or {}
    raw_next_uri = next_page.get("uri")
    next_page_uri = urljoin(HOST, raw_next_uri) if raw_next_uri else None
    next_offset = next_page.get("offset") or _offset_from_uri(next_page_uri)

    valid_tenders = [tender for tender in tenders if isinstance(tender, dict)]
    
    # Use gather to resolve the N+1 problem
    detail_tasks = {
        tender.get("id"): asyncio.create_task(
            fetch_tender_details(session, tender["id"], semaphore)
        )
        for tender in valid_tenders if tender.get("id")
    }
    task_ids = list(detail_tasks)
    detail_results = await asyncio.gather(
        *(detail_tasks[tender_id] for tender_id in task_ids), return_exceptions=True
    )
    details_by_id = dict(zip(task_ids, detail_results))

    extracted_at = utc_now_iso()
    feed_records: list[dict[str, Any]] = []
    medical_tenders: list[dict[str, Any]] = []
    max_modified = 0.0

    # Distribute data into two streams according to the specification
    for tender in valid_tenders:
        tender_id = tender.get("id", "")
        details = details_by_id.get(tender_id)
        
        if isinstance(details, Exception):
            logging.error("Detailed request failed for tender %s: %s", tender_id, details)
            details = None
            
        found_cpv = extract_matching_cpv(details, cpv_pattern) if isinstance(details, dict) else ""
        modified = public_modified_timestamp(tender)
        max_modified = max(max_modified, modified)
        
        feed_records.append(
            {
                "id": tender_id,
                "dateModified": tender.get("dateModified", ""),
                "public_modified": modified,
                "source_page_uri": page_uri,
                "extracted_at": extracted_at,
                "found_CPV": found_cpv,
            }
        )
        
        if found_cpv and isinstance(details, dict):
            medical_tenders.append(details)

    return FeedPageResult(
        feed_records=feed_records,
        medical_tenders=medical_tenders,
        next_page_uri=next_page_uri,
        next_offset=next_offset,
        max_public_modified=max_modified,
    )


def _offset_from_uri(uri: str | None) -> str | None:
    """Extract the pagination cursor (offset) from a Prozorro URI."""
    if not uri:
        return None
    return parse_qs(urlparse(uri).query).get("offset", [None])[0]


def _filename_timestamp(offset: str | None, fallback: float) -> int:
    """Convert the offset cursor to a timestamp for filename generation."""
    if offset:
        match = re.match(r"^(\d+(?:\.\d+)?)", str(offset))
        if match:
            try:
                return int(float(match.group(1)))
            except ValueError:
                pass
    return int(fallback or datetime.now(timezone.utc).timestamp())


class S3StateManager:
    """Execution state management (Checkpointing) via AWS S3."""

    def __init__(
        self, bucket_name: str, s3_client: Any | None = None, state_key: str = STATE_KEY
    ) -> None:
        self.bucket_name = bucket_name
        self.s3_client = s3_client or boto3.client("s3")
        self.state_key = state_key

    def load_state(self) -> dict[str, Any]:
        """Load the last saved checkpoint."""
        try:
            response = self.s3_client.get_object(Bucket=self.bucket_name, Key=self.state_key)
        except ClientError as error:
            error_code = error.response.get("Error", {}).get("Code")
            if error_code in ("NoSuchKey", "404", "NotFound"):
                logging.info("S3 checkpoint not found; starting from the beginning (feed head).")
                return {
                    "last_offset": None,
                    "total_items_processed": 0,
                    "medical_items_found": 0,
                }
            raise
        state = json.loads(response["Body"].read().decode("utf-8"))
        state.setdefault("last_offset", None)
        state.setdefault("total_items_processed", 0)
        state.setdefault("medical_items_found", 0)
        return state

    def save_state(self, state: dict[str, Any]) -> None:
        """Atomically save a new checkpoint object to S3."""
        payload = dict(state)
        payload["last_updated_at"] = utc_now_iso()
        self.s3_client.put_object(
            Bucket=self.bucket_name,
            Key=self.state_key,
            Body=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
            ContentType="application/json",
        )


class ProzorroPipelineRunner:
    """Orchestrator that executes the full cycle of downloading, buffering, and S3 Hive partitioning."""

    def __init__(
        self,
        bucket_name: str,
        page_size: int = 100,
        flush_every_pages: int = FLUSH_EVERY_PAGES,
        cpv_pattern: re.Pattern[str] = MEDICAL_CPV_PATTERN,
        s3_client: Any | None = None,
        temp_dir: str = "/tmp",
        mode: str | None = None,
        opt_fields: list[str] | None = None,
        descending: str | int | None = None,
    ) -> None:
        self.bucket_name = bucket_name
        self.page_size = page_size
        self.flush_every_pages = max(1, flush_every_pages)
        self.cpv_pattern = cpv_pattern
        self.s3_client = s3_client or boto3.client("s3")
        self.state_manager = S3StateManager(bucket_name, self.s3_client)
        self.temp_dir = temp_dir
        self.mode = mode
        self.opt_fields = opt_fields
        self.descending = descending

    async def _upload_file(self, path: str, key: str) -> None:
        """Asynchronous upload of a local file to S3 via to_thread."""
        logging.info("Uploading %s to s3://%s/%s", path, self.bucket_name, key)
        await asyncio.to_thread(
            self.s3_client.upload_file, path, self.bucket_name, key
        )

    async def run(self) -> dict[str, int]:
        """Main execution loop: parsing feed pages until data becomes an empty array."""
        state = await asyncio.to_thread(self.state_manager.load_state)
        last_offset = state.get("last_offset")
        
        page_uri = build_feed_url(
            offset=last_offset,
            limit=self.page_size,
            mode=self.mode,
            opt_fields=self.opt_fields,
            descending=self.descending,
        )
        
        total_items = int(state.get("total_items_processed", 0))
        total_medical = int(state.get("medical_items_found", 0))
        run_items = 0
        run_medical = 0
        pages_since_flush = 0
        buffered_items = 0
        buffered_medical = 0
        fallback_timestamp = 0.0

        os.makedirs(self.temp_dir, exist_ok=True)
        feed_fd, feed_path = tempfile.mkstemp(prefix="prozorro_feed_", suffix=".jsonl", dir=self.temp_dir)
        medical_fd, medical_path = tempfile.mkstemp(
            prefix="prozorro_medical_", suffix=".jsonl", dir=self.temp_dir
        )
        os.close(feed_fd)
        os.close(medical_fd)
        
        # Concurrency limit
        semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
        timeout = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT_SECONDS)
        connector = aiohttp.TCPConnector(limit=MAX_CONCURRENT_REQUESTS + 1)

        async def flush_batch(offset_for_name: str | None) -> None:
            """Synchronize (flush) current files to S3 and update the checkpoint."""
            nonlocal buffered_items, buffered_medical, state
            if buffered_items == 0:
                return
            
            partition_date = datetime.now(timezone.utc)
            # Hive partitioning: year=YYYY/month=MM/day=DD
            partition = (
                f"year={partition_date:%Y}/month={partition_date:%m:02d}/day={partition_date:%d:02d}"
            )
            file_timestamp = _filename_timestamp(offset_for_name, fallback_timestamp)
            
            feed_key = f"raw/feed_tenders/{partition}/feed_tenders_{file_timestamp}.jsonl"
            await self._upload_file(feed_path, feed_key)
            
            if buffered_medical:
                medical_key = f"raw/medical_tenders/{partition}/medical_tenders_{file_timestamp}.jsonl"
                await self._upload_file(medical_path, medical_key)

            new_state = {
                "last_offset": last_offset,
                "total_items_processed": total_items,
                "medical_items_found": total_medical,
            }
            await asyncio.to_thread(self.state_manager.save_state, new_state)
            state = new_state
            
            # Clear buffer files
            open(feed_path, "w", encoding="utf-8").close()
            open(medical_path, "w", encoding="utf-8").close()
            
            buffered_items = 0
            buffered_medical = 0

        try:
            async with aiohttp.ClientSession(
                timeout=timeout, connector=connector
            ) as session:
                while True:
                    logging.info("Processing feed page: %s", page_uri)
                    page = await process_feed_page(
                        session, page_uri, semaphore, self.cpv_pattern
                    )
                    
                    # Stop when reaching the head of the feed
                    if not page.feed_records:
                        logging.info("Reached feed head; no new tenders.")
                        break

                    with open(feed_path, "a", encoding="utf-8") as feed_file:
                        for record in page.feed_records:
                            feed_file.write(json.dumps(record, ensure_ascii=False) + "\n")
                            
                    if page.medical_tenders:
                        with open(medical_path, "a", encoding="utf-8") as medical_file:
                            for tender in page.medical_tenders:
                                medical_file.write(json.dumps(tender, ensure_ascii=False) + "\n")

                    page_item_count = len(page.feed_records)
                    page_medical_count = len(page.medical_tenders)
                    
                    run_items += page_item_count
                    run_medical += page_medical_count
                    total_items += page_item_count
                    total_medical += page_medical_count
                    buffered_items += page_item_count
                    buffered_medical += page_medical_count
                    pages_since_flush += 1
                    fallback_timestamp = page.max_public_modified or fallback_timestamp

                    if page.next_offset:
                        last_offset = page.next_offset
                        
                    if pages_since_flush >= self.flush_every_pages:
                        await flush_batch(page.next_offset or last_offset)
                        pages_since_flush = 0
                        
                    if not page.next_page_uri:
                        logging.info("Feed page has no next_page_uri link; stopping traversal.")
                        break
                    page_uri = page.next_page_uri

            await flush_batch(last_offset)
            logging.info(
                "Extraction completed: %s feed records, %s medical tenders",
                run_items, run_medical,
            )
            return {"items_processed": run_items, "medical_items_found": run_medical}
            
        finally:
            # Guaranteed removal of temporary files
            for path in (feed_path, medical_path):
                try:
                    os.remove(path)
                except FileNotFoundError:
                    pass


async def main() -> None:
    """Entry point for the S3 ETL pipeline configured via environment variables."""
    bucket_name = os.getenv("AWS_S3_BUCKET", "prozorro-data-lake")
    page_size = int(os.getenv("PROZORRO_PAGE_SIZE", "100"))
    flush_every_pages = int(os.getenv("PROZORRO_FLUSH_EVERY_PAGES", str(FLUSH_EVERY_PAGES)))
    
   # Fetching optional parameters from the environment
    mode = os.getenv("PROZORRO_MODE")
    opt_fields_raw = os.getenv("PROZORRO_OPT_FIELDS")
    opt_fields = opt_fields_raw.split(",") if opt_fields_raw else None
    descending = os.getenv("PROZORRO_DESCENDING")
    
    runner = ProzorroPipelineRunner(
        bucket_name=bucket_name,
        page_size=page_size,
        flush_every_pages=flush_every_pages,
        mode=mode,
        opt_fields=opt_fields,
        descending=descending,
    )
    await runner.run()


if __name__ == "__main__":
    asyncio.run(main())
