#!/usr/bin/env -S uv run --upgrade

import argparse
import logging
import queue
import re
import sys
import threading
import time  # Import time at the top level
from datetime import datetime
from pathlib import Path

import requests  # Import requests to catch its exceptions
from markitdown import MarkItDown

# --- Constants ---
BASE_WIKIPEDIA_URL = "https://en.m.wikipedia.org/wiki/Portal:Current_events/"
DEFAULT_OUTPUT_DIR = "./docs/_posts/"
LOG_FILE = "wikipedia_news_downloader.log"
RETRY_MAX_ATTEMPTS = 5
RETRY_BASE_WAIT_SECONDS = 20  # Start wait time for retries
MIN_MARKDOWN_LENGTH_PUBLISH = 10  # Minimum length to consider content valid for publishing
# --- End Constants ---


def setup_logging(verbose: bool = False) -> logging.Logger:
    """Configure logging based on verbosity level."""
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


MONTH_NAME_TO_NUMBER = {
    "January": 1,
    "February": 2,
    "March": 3,
    "April": 4,
    "May": 5,
    "June": 6,
    "July": 7,
    "August": 8,
    "September": 9,
    "October": 10,
    "November": 11,
    "December": 12,
}


def _clean_daily_markdown_content(daily_md: str) -> str:
    """Helper to apply common cleaning rules to daily markdown."""
    # Replace relative links with absolute links
    cleaned_text = re.sub(r"\(/wiki/", r"(https://en.wikipedia.org/wiki/", daily_md)
    # Remove markdown links to non-existent pages (redlinks), leaving only the text
    cleaned_text = re.sub(
        r'\[([^\]]+)\]\(/w/index\.php\?title=[^&\s]+&action=edit&redlink=1\s*"[^"]*"\)',
        r"\1",
        cleaned_text,
    )
    # Remove any text that looks like [[1]](#cite_note-1)
    cleaned_text = re.sub(r"\[\[\d+\]\]\(#cite_note-\d+\)", "", cleaned_text)

    # Remove trailing spaces or tabs from the end of each individual line.
    # This doesn't remove empty lines but cleans lines that only contained spaces/tabs.
    cleaned_text = re.sub(r"[ \t]+$", "", cleaned_text, flags=re.MULTILINE)

    # Ensure the entire text block ends with a single newline and has no other trailing whitespace.
    # This also handles cases where the original text might not end with a newline,
    # or collapses multiple trailing newlines from the block into one.
    return cleaned_text.rstrip() + "\n"


def split_and_clean_monthly_markdown(monthly_markdown: str, month_datetime: datetime, logger: logging.Logger) -> list[tuple[datetime, str]]:
    """Split markdown by day.

    Splits markdown from a monthly Wikipedia Current Events page into daily segments,
    extracts dates, and cleans each segment.
    """
    # Remove the trailing text that is not part of the daily events.
    monthly_markdown = re.sub(r"\[◀\].*", "", monthly_markdown, flags=re.DOTALL)

    daily_events = []
    # Regex to find the start of each day's content block, capturing month name, day, and year.
    # Example: June 1, 2025 (2025-06-01) (Sunday) ... [watch]
    # \xa0 is a non-breaking space often found in Wikipedia dates.
    # Regex to capture Month, Day, Year from a line like:
    # June 1, 2025 (2025-06-01) (Sunday)
    # followed by lines for edit, history, watch links.
    # The content for the day starts AFTER the "* [watch](...)\n" line.
    day_delimiter_regex = re.compile(
        r"([A-Za-z]+)\xa0(\d{1,2}),\xa0(\d{4})(?:[^\n]*)\n\n"  # Month Day, Year line (captures M, D, Y), rest of line, then two newlines
        r"\* \[edit\]\(.*?\)\n"  # * [edit](...) line
        r"\* \[history\]\(.*?\)\n"  # * [history](...) line
        r"\* \[watch\]\(.*?\)\n",  # * [watch](...) line
    )

    # Find all starting positions of daily segments
    matches = list(day_delimiter_regex.finditer(monthly_markdown))
    logger.debug(f"Found {len(matches)} potential daily segments in markdown for {month_datetime.strftime('%Y-%B')}.")

    for i, match in enumerate(matches):
        try:
            month_str, day_str, year_str = match.groups()
            day = int(day_str)
            year = int(year_str)
            month = MONTH_NAME_TO_NUMBER.get(month_str)

            if not month:
                logger.warning(f"Could not parse month: {month_str} for segment {i + 1}. Skipping.")
                continue

            day_dt = datetime(year, month, day)
            logger.debug(f"Extracted date for segment {i + 1}: {day_dt.strftime('%Y-%m-%d')}")

            # Determine the content of the current day's segment
            segment_start_pos = match.end()  # Content starts after the matched delimiter
            if i + 1 < len(matches):
                # Next day's header starts where this day's content ends
                segment_end_pos = matches[i + 1].start()
            else:
                # This is the last segment, so it goes to the end of the markdown
                segment_end_pos = len(monthly_markdown)

            daily_raw_content = monthly_markdown[segment_start_pos:segment_end_pos]

            # The header (e.g. "June 1, 2025 ... (action=watch)\n") has already been excluded by segment_start_pos.
            # Now apply further cleaning.
            cleaned_daily_md = _clean_daily_markdown_content(daily_raw_content.strip())

            if cleaned_daily_md.strip():  # Ensure there's some content
                daily_events.append((day_dt, cleaned_daily_md))
            else:
                logger.debug(f"Segment {i + 1} for {day_dt.strftime('%Y-%m-%d')} is empty after cleaning. Skipping.")

        except ValueError:
            logger.exception(f"Error parsing date for segment {i + 1} ({month_str} {day_str}, {year_str}). Skipping.")
            continue
        except Exception:
            logger.exception(f"Unexpected error processing segment {i + 1} for {month_datetime.strftime('%Y-%B')}")
            continue

    logger.info(f"Successfully processed {len(daily_events)} daily segments from {month_datetime.strftime('%Y-%B')}.")
    return daily_events


def save_news(date: datetime, full_content: str, output_dir: str, logger: logging.Logger) -> None:
    """Save markdown to specified directory."""
    logger.info(f"Preparing to save news for {date}")

    # Create date-specific folder
    folder_path = "./docs/_posts/"
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    logger.debug(f"Created folder: {folder_path}")

    # Save markdown
    markdown_path = Path(output_dir) / (date.strftime("%Y-%m-%d") + "-index.md")
    with markdown_path.open("w", encoding="utf-8", newline="\n") as f:
        f.write(full_content)
    logger.info(f"Saved markdown to: {markdown_path}")


def generate_jekyll_content(date: datetime, markdown_body: str, logger: logging.Logger) -> str | None:
    """Jekyll front matter generator.

    Generates the full page content with Jekyll front matter.
    Returns the full content string, or None if markdown_body is invalid.
    """
    # Determine published status based on markdown content
    is_published = len(markdown_body) >= MIN_MARKDOWN_LENGTH_PUBLISH

    if not is_published:
        logger.warning(f"Markdown for {date.strftime('%Y-%m-%d')} is too short ({len(markdown_body)=}). Setting published: false.")

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
        "",
    ]

    # Combine front matter and content (use empty body if not published)
    content_body = markdown_body if is_published else ""
    return "\n".join(front_matter_lines) + content_body


def worker(date_queue, output_dir, logger) -> None:
    while True:
        try:
            item = date_queue.get(timeout=2)
        except queue.Empty:
            break  # No more items to process

        # 'date' here is the first day of the month to be processed
        month_date, retries = item
        if retries > RETRY_MAX_ATTEMPTS:
            logger.error(f"Exceeded max retries ({RETRY_MAX_ATTEMPTS}) for month {month_date.strftime('%Y-%B')}")
            date_queue.task_done()
            continue

        logger.info(f"Processing month: {month_date.strftime('%Y-%B')} (attempt {retries + 1})")
        # URL is for the entire month, e.g., Portal:Current_events/January_2025
        url = f"{BASE_WIKIPEDIA_URL}{month_date:%B}_{month_date.year}"
        logger.debug(f"Prepare to fetch monthly page: {url}")

        try:
            monthly_raw_markdown = ""
            try:
                md = MarkItDown()
                result = md.convert(url)
                monthly_raw_markdown = result.text_content
                logger.debug(f"Raw MarkItDown result length for month {month_date.strftime('%Y-%B')}: {len(monthly_raw_markdown)}")
            except requests.exceptions.RequestException as e:
                status_code = getattr(e.response, "status_code", None)
                if status_code == 429:
                    logger.warning(
                        (
                            f"HTTP 429 Too Many Requests for {url}. "
                            f"Re-queuing month {month_date.strftime('%Y-%B')} for later retry "
                            f"(attempt {retries + 1}/{RETRY_MAX_ATTEMPTS})..."
                        ),
                    )
                    date_queue.put((month_date, retries + 1))  # Re-queue with incremented retry count
                    date_queue.task_done()
                    continue
                if status_code == 404:
                    logger.warning(f"HTTP 404 Not Found for {url} (month: {month_date.strftime('%Y-%B')}). Skipping retries.")
                    date_queue.task_done()
                    continue
                logger.warning(
                    f"Request error fetching {url} (month: {month_date.strftime('%Y-%B')}): {e}. "
                    f"Retrying (attempt {retries + 1}/{RETRY_MAX_ATTEMPTS})...",
                )
                time.sleep(RETRY_BASE_WAIT_SECONDS / 2 * (2**retries))
                date_queue.put((month_date, retries + 1))
                date_queue.task_done()
                continue
            except Exception:  # Includes MarkItDown conversion errors
                logger.exception(
                    f"Unexpected error converting {url} (month: {month_date.strftime('%Y-%B')}, "
                    f"attempt {retries + 1}/{RETRY_MAX_ATTEMPTS})",
                )
                time.sleep(RETRY_BASE_WAIT_SECONDS / 2 * (2**retries))
                date_queue.put((month_date, retries + 1))
                date_queue.task_done()
                continue

            # If we get here, monthly_raw_markdown is available
            logger.info(f"Successfully fetched content for month: {month_date.strftime('%Y-%B')}")

            daily_events = split_and_clean_monthly_markdown(monthly_raw_markdown, month_date, logger)

            if not daily_events:
                logger.warning(
                    f"No daily events found or extracted for month: {month_date.strftime('%Y-%B')}. "
                    f"This might be normal for very recent archives or if the content structure changed.",
                )

            for event_date, daily_markdown in daily_events:
                logger.info(f"Processing extracted day: {event_date.strftime('%Y-%m-%d')} from month {month_date.strftime('%Y-%B')}")
                full_content = generate_jekyll_content(event_date, daily_markdown, logger)
                if full_content and "published: true" in full_content:
                    save_news(event_date, full_content, output_dir, logger)
                else:
                    logger.warning(
                        f"Skipping save for {event_date.strftime('%Y-%m-%d')}: Content marked as unpublished or generation failed.",
                    )

        except Exception:
            # This is a general catch-all for errors not handled by the retry logic for fetching/conversion,
            # such as unexpected errors within split_and_clean_monthly_markdown or the daily processing loop.
            logger.exception(f"Unexpected error processing month {month_date.strftime('%Y-%B')}")
        finally:
            date_queue.task_done()


def main() -> None:
    # Set up argument parsing
    parser = argparse.ArgumentParser(description="Download Wikipedia News")
    parser.add_argument("-v", "--verbose", action="store_true", help="Enable verbose logging")
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

    # Dates to download
    now = datetime.now()
    # Start from January 1, 2025
    start_date = datetime(2025, 1, 1)
    # End on the first day of the current month
    end_date = datetime(now.year, now.month, 1)

    # Use a set to avoid duplicates, populate with the first day of each month
    dates: set[datetime] = set()
    current_date = start_date
    while current_date <= end_date:
        dates.add(current_date)
        # Move to the first day of the next month
        if current_date.month == 12:
            current_date = datetime(current_date.year + 1, 1, 1)
        else:
            current_date = datetime(current_date.year, current_date.month + 1, 1)

    logger.info(
        f"Starting download for {len(dates)} dates from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}"
        f" (first day of each month inclusive) using up to {args.workers or 'default'} workers."
        f" Output directory: {args.output_dir}",
    )

    # Ensure output directory exists
    Path(args.output_dir).mkdir(parents=True, exist_ok=True)

    # Use a queue for cooperative retry handling
    date_queue: queue.Queue[tuple[datetime, int]] = queue.Queue()
    for date in sorted(dates):
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
