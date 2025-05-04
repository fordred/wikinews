# /// script
# requires-python = "==3.13.*"
# dependencies = [
#     "markitdown",
# ]
# ///

from typing import Optional, Set
import argparse
from datetime import datetime, timedelta
import logging
from markitdown import MarkItDown
import requests  # Import requests to catch its exceptions
import os
import re
import sys
import concurrent.futures
import time  # Import time at the top level
import queue
import threading

# --- Constants ---
BASE_WIKIPEDIA_URL = "https://en.m.wikipedia.org/wiki/Portal:Current_events/"
DEFAULT_OUTPUT_DIR = "./docs/_posts/"
LOG_FILE = "wikipedia_news_downloader.log"
RETRY_MAX_ATTEMPTS = 5
RETRY_BASE_WAIT_SECONDS = 20  # Start wait time for retries
MIN_MARKDOWN_LENGTH_PUBLISH = (
    10  # Minimum length to consider content valid for publishing
)
# --- End Constants ---


def setup_logging(verbose=False):
    """
    Configure logging based on verbosity level.
    """
    log_level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s - %(levelname)s: %(message)s",
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(LOG_FILE, mode="w"),
        ],
    )
    return logging.getLogger(__name__)


def clean_wikipedia_markdown(raw_markdown: str, logger: logging.Logger) -> str:
    """Cleans the raw markdown extracted from Wikipedia Current Events."""
    # Remove text up to and including the line that begins with "[watch]"
    cleaned_text = re.sub(r"^.*action=watch\)\n", "", raw_markdown, flags=re.DOTALL)
    # Remove text starting at "[Month" until the end
    cleaned_text = re.sub(r"\[Month.*", "", cleaned_text, flags=re.DOTALL)
    # Replace relative links with absolute links
    cleaned_text = re.sub(r"\(/wiki/", r"(https://en.wikipedia.org/wiki/", cleaned_text)
    # Remove markdown links to non-existent pages (redlinks), leaving only the text
    cleaned_text = re.sub(
        r'\[([^\]]+)\]\(/w/index\.php\?title=[^&\s]+&action=edit&redlink=1\s*"[^"]*"\)',
        r"\1",
        cleaned_text,
    )
    # Remove trailing whitespace and newlines, ensure single trailing newline
    cleaned_text = cleaned_text.rstrip() + "\n"

    logger.debug(f"Cleaned MarkItDown result head: {cleaned_text[:100]}")
    logger.debug(f"Cleaned MarkItDown result tail: {cleaned_text[-100:]}")
    return cleaned_text


def save_news(
    date: datetime, full_content: str, output_dir: str, logger: logging.Logger
):
    """
    Save markdown to specified directory.
    """
    logger.info(f"Preparing to save news for {date}")

    # Create date-specific folder
    folder_path = "./docs/_posts/"
    os.makedirs(output_dir, exist_ok=True)
    logger.debug(f"Created folder: {folder_path}")

    # Save markdown
    markdown_path = os.path.join(output_dir, date.strftime("%Y-%m-%d") + "-index.md")
    with open(markdown_path, "w", encoding="utf-8", newline="\n") as f:
        f.write(full_content)
    logger.info(f"Saved markdown to: {markdown_path}")


def generate_jekyll_content(
    date: datetime, markdown_body: str, logger: logging.Logger
) -> Optional[str]:
    """
    Generates the full page content with Jekyll front matter.
    Returns the full content string, or None if markdown_body is invalid.
    """
    # Determine published status based on markdown content
    is_published = len(markdown_body) >= MIN_MARKDOWN_LENGTH_PUBLISH

    if not is_published:
        logger.warning(
            f"Markdown for {date.strftime('%Y-%m-%d')} is too short ({len(markdown_body)=}). Setting published: false."
        )

    # Build front matter
    front_matter_lines = [
        "---",
        "layout: post",
        f"title: {date.strftime('%Y %B %d')}",
        f"date: {date.strftime('%Y-%m-%d')}",
        f"published: {'true' if is_published else 'false'}",
        "---",
        "",  # Add blank lines after front matter
        "",
    ]

    # Combine front matter and content (use empty body if not published)
    content_body = markdown_body if is_published else ""
    full_content = "\n".join(front_matter_lines) + content_body

    return full_content


def worker(date_queue, output_dir, logger):
    while True:
        try:
            item = date_queue.get(timeout=2)
        except queue.Empty:
            break  # No more items to process

        date, retries = item
        if retries > RETRY_MAX_ATTEMPTS:
            logger.error(
                f"Exceeded max retries ({RETRY_MAX_ATTEMPTS}) for {date.strftime('%Y-%m-%d')}"
            )
            date_queue.task_done()
            continue

        logger.info(
            f"Processing date: {date.strftime('%Y-%m-%d')} (attempt {retries + 1})"
        )
        url = f"{BASE_WIKIPEDIA_URL}{date.year}_{date:%B}_{date.day}"
        logger.debug(f"Prepare to fetch page: {url}")

        try:
            md = MarkItDown()
            try:
                result = md.convert(url)
                logger.debug(
                    f"Raw MarkItDown result length: {len(result.text_content)}"
                )
                markdown_body = clean_wikipedia_markdown(result.text_content, logger)
            except requests.exceptions.RequestException as e:
                status_code = getattr(e.response, "status_code", None)
                if status_code == 429:
                    logger.warning(
                        f"HTTP 429 Too Many Requests for {url}. Re-queuing for later retry (attempt {retries + 1}/{RETRY_MAX_ATTEMPTS})..."
                    )
                    # Re-queue with incremented retry count
                    date_queue.put((date, retries + 1))
                    date_queue.task_done()
                    continue
                elif status_code == 404:
                    logger.warning(f"HTTP 404 Not Found for {url}. Skipping retries.")
                    date_queue.task_done()
                    continue
                else:
                    logger.warning(
                        f"Request error fetching {url}: {e}. Retrying (attempt {retries + 1}/{RETRY_MAX_ATTEMPTS})..."
                    )
                    time.sleep(RETRY_BASE_WAIT_SECONDS / 2 * (2**retries))
                    date_queue.put((date, retries + 1))
                    date_queue.task_done()
                    continue
            except Exception as e:
                logger.exception(
                    f"Unexpected error converting {url} (attempt {retries + 1}/{RETRY_MAX_ATTEMPTS}): {e}"
                )
                time.sleep(RETRY_BASE_WAIT_SECONDS / 2 * (2**retries))
                date_queue.put((date, retries + 1))
                date_queue.task_done()
                continue

            # If we get here, markdown_body is available
            logger.info(
                f"Successfully fetched and converted content for {date.strftime('%Y-%m-%d')}"
            )
            full_content = generate_jekyll_content(date, markdown_body, logger)
            if full_content and "published: true" in full_content:
                save_news(date, full_content, output_dir, logger)
            else:
                logger.warning(
                    f"Skipping save for {date.strftime('%Y-%m-%d')}: Content marked as unpublished or generation failed."
                )
        except Exception as e:
            logger.exception(
                f"Unexpected error processing date {date.strftime('%Y-%m-%d')}"
            )
        finally:
            date_queue.task_done()


def main():
    # Set up argument parsing
    parser = argparse.ArgumentParser(description="Download Wikipedia News")
    parser.add_argument(
        "-v", "--verbose", action="store_true", help="Enable verbose logging"
    )
    parser.add_argument(
        "-a",
        "--all",
        action="store_true",
        help="Download news from Jan 1, 2025, to today",
    )
    parser.add_argument(
        "-o",
        "--output-dir",
        type=str,
        default=DEFAULT_OUTPUT_DIR,
        help=f"Directory to save markdown files (default: {DEFAULT_OUTPUT_DIR})",
    )
    parser.add_argument(
        "-w",
        "--workers",
        type=int,
        default=None,  # Default to ThreadPoolExecutor's choice
        help="Maximum number of concurrent download workers",
    )
    args = parser.parse_args()

    # Setup logging
    logger = setup_logging(args.verbose)

    # Dates to download (local time, no NZ-specific code)
    now = datetime.now()
    today = datetime(now.year, now.month, now.day)
    tomorrow = today + timedelta(days=1)

    if args.all:
        logger.info("Downloading all news from Jan 1, 2025, to tomorrow (inclusive)")
        start_date = datetime(2025, 1, 1)
    else:
        logger.info("Downloading news for the last 7 days up to tomorrow (inclusive)")
        start_date = today - timedelta(days=7)

    end_date = tomorrow + timedelta(days=1)  # inclusive of tomorrow

    # Use a set to avoid duplicates
    dates: Set[datetime] = set()
    current_date = start_date
    while current_date < end_date:
        dates.add(current_date)
        current_date += timedelta(days=1)

    logger.info(
        f"Starting download for {len(dates)} dates from {start_date} to {end_date}"
        f" using up to {args.workers or 'default'} workers."
        f" Output directory: {args.output_dir}"
    )

    # Ensure output directory exists
    os.makedirs(args.output_dir, exist_ok=True)

    # Use a queue for cooperative retry handling
    date_queue = queue.Queue()
    for date in sorted(list(dates)):
        date_queue.put((date, 0))  # (date, retry_count)

    num_workers = args.workers or min(8, len(dates))
    threads = []
    for _ in range(num_workers):
        t = threading.Thread(target=worker, args=(date_queue, args.output_dir, logger))
        t.start()
        threads.append(t)

    date_queue.join()
    for t in threads:
        t.join()

    logger.info("Wikipedia News Download Complete")


if __name__ == "__main__":
    main()
