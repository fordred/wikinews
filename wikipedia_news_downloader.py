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

# Precompiled regex patterns
RELATIVE_WIKI_LINK_RE = re.compile(r"\(/wiki/")
REDLINK_RE = re.compile(r'\[([^\]]+)\]\(/w/index\.php\?title=[^&\s]+&action=edit&redlink=1\s*"[^"]*"\)')
CITATION_LINK_RE = re.compile(r"\[\[\d+\]\]\(#cite_note-\d+\)")
TRAILING_SPACES_RE = re.compile(r"[ \t]+$", flags=re.MULTILINE)
BOLD_HEADINGS_RE = re.compile(r"^\*\*(.*?)\*\*$", flags=re.MULTILINE)
PLUS_LIST_RE = re.compile(r"^ {2}\+", flags=re.MULTILINE)
DASH_LIST_RE = re.compile(r"^ {4}\-", flags=re.MULTILINE)


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


def clean_daily_markdown_content(daily_md: str) -> str:
    """Helper to apply common cleaning rules to daily markdown."""
    # Replace relative links with absolute links
    cleaned_text = RELATIVE_WIKI_LINK_RE.sub(r"(https://en.wikipedia.org/wiki/", daily_md)
    # Remove markdown links to non-existent pages (redlinks), leaving only the text
    cleaned_text = REDLINK_RE.sub(
        r"\1",
        cleaned_text,
    )
    # Remove any text that looks like [[1]](#cite_note-1)
    cleaned_text = CITATION_LINK_RE.sub("", cleaned_text)

    # Remove trailing spaces or tabs from the end of each individual line.
    # This doesn't remove empty lines but cleans lines that only contained spaces/tabs.
    cleaned_text = TRAILING_SPACES_RE.sub("", cleaned_text)

    # Turn bold headings into H4 headings.
    cleaned_text = BOLD_HEADINGS_RE.sub(r"#### \1", cleaned_text)
    cleaned_text = PLUS_LIST_RE.sub("  *", cleaned_text)
    cleaned_text = DASH_LIST_RE.sub("    *", cleaned_text)

    # Ensure the entire text block ends with a single newline and has no other trailing whitespace.
    # This also handles cases where the original text might not end with a newline,
    # or collapses multiple trailing newlines from the block into one.
    return cleaned_text.rstrip() + "\n"


# Regex to find the start of each day's content block, capturing month name, day, and year.
# Example: June 1, 2025 (2025-06-01) (Sunday) ... [watch]
# \xa0 is a non-breaking space often found in Wikipedia dates.
# Regex to capture Month, Day, Year from a line like:
# June 1, 2025 (2025-06-01) (Sunday)
# followed by lines for edit, history, watch links.
# The content for the day starts AFTER the "* [watch](...)\n" line.
DAY_DELIMITER_RE = re.compile(
    r"([A-Za-z]+)\xa0(\d{1,2}),\xa0(\d{4})(?:[^\n]*)\n\n"  # Month Day, Year line (captures M, D, Y), rest of line, then two newlines
    r"\* \[edit\]\(.*?\)\n"  # * [edit](...) line
    r"\* \[history\]\(.*?\)\n"  # * [history](...) line
    r"\* \[watch\]\(.*?\)\n",  # * [watch](...) line
)
MONTHLY_MARKDOWN_RE = re.compile(r"\[◀\].*", flags=re.DOTALL)


def split_and_clean_monthly_markdown(monthly_markdown: str, month_datetime: datetime, logger: logging.Logger) -> list[tuple[datetime, str]]:
    """Split markdown by day.

    Splits markdown from a monthly Wikipedia Current Events page into daily segments,
    extracts dates, and cleans each segment.
    """
    # Remove the trailing text that is not part of the daily events.
    monthly_markdown = MONTHLY_MARKDOWN_RE.sub("", monthly_markdown)

    daily_events: list[tuple[datetime, str]] = []
    # Find all starting positions of daily segments
    matches = list(DAY_DELIMITER_RE.finditer(monthly_markdown))
    logger.debug(f"Found {len(matches)} potential daily segments in markdown for {month_datetime.strftime('%Y-%B')}.")

    for i, match in enumerate(matches):
        month_str = day_str = year_str = None  # Ensure variables are always defined
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
            cleaned_daily_md = clean_daily_markdown_content(daily_raw_content.strip())

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


def generate_jekyll_content(date: datetime, markdown_body: str, logger: logging.Logger) -> str:
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


from typing import Union, Tuple

# Define a more generic queue item type
QueueItem = Union[Tuple[datetime, int], Path]

def worker(processing_queue: queue.Queue[QueueItem], output_dir: str, logger: logging.Logger) -> None:
    while True:
        try:
            queue_item = processing_queue.get(timeout=2)
        except queue.Empty:
            break  # No more items to process

        monthly_raw_markdown = ""
        source_name = "" # For logging
        month_for_processing: datetime # datetime object representing the month being processed.

        # --- Determine source and fetch/read content ---
        if isinstance(queue_item, Path): # Processing a local HTML file
            local_file_path = queue_item
            source_name = local_file_path.name
            logger.info(f"Processing local file: {source_name}")

            # Derive month_for_processing from filename, e.g., "january_2025.html"
            try:
                parts = local_file_path.stem.split("_")
                month_name_str = parts[0].capitalize()
                year_str = parts[1]
                month_num = MONTH_NAME_TO_NUMBER[month_name_str]
                month_for_processing = datetime(int(year_str), month_num, 1)
            except (IndexError, KeyError, ValueError) as e:
                logger.error(f"Could not derive date from filename {source_name}: {e}. Skipping.")
                processing_queue.task_done()
                continue

            try:
                md = MarkItDown()
                # Use file URI scheme for local files
                result = md.convert(f"file://{local_file_path.resolve()}")
                monthly_raw_markdown = result.text_content
                logger.debug(f"Raw MarkItDown result length for {source_name}: {len(monthly_raw_markdown)}")
            except Exception: # Catch all exceptions during local file processing
                logger.exception(f"Unexpected error converting local file {source_name}")
                processing_queue.task_done()
                continue # Skip to next item

        elif isinstance(queue_item, tuple): # Processing a date (fetch from URL)
            month_date, retries = queue_item
            month_for_processing = month_date # Use the provided datetime
            source_name = month_date.strftime('%Y-%B')

            if retries > RETRY_MAX_ATTEMPTS:
                logger.error(f"Exceeded max retries ({RETRY_MAX_ATTEMPTS}) for month {source_name}")
                processing_queue.task_done()
                continue

            logger.info(f"Processing month: {source_name} (attempt {retries + 1})")
            url = f"{BASE_WIKIPEDIA_URL}{month_date:%B}_{month_date.year}"
            logger.debug(f"Prepare to fetch monthly page: {url}")

            try:
                md = MarkItDown()
                result = md.convert(url)
                monthly_raw_markdown = result.text_content
                logger.debug(f"Raw MarkItDown result length for month {source_name}: {len(monthly_raw_markdown)}")
            except requests.exceptions.RequestException as e:
                status_code = getattr(e.response, "status_code", None)
                if status_code == 429: # Too Many Requests
                    logger.warning(
                        f"HTTP 429 Too Many Requests for {url}. Re-queuing {source_name} (attempt {retries + 1})."
                    )
                    processing_queue.put((month_date, retries + 1))
                elif status_code == 404: # Not Found
                    logger.warning(f"HTTP 404 Not Found for {url} ({source_name}). Skipping.")
                else: # Other request errors
                    logger.warning(
                        f"Request error fetching {url} ({source_name}): {e}. Retrying (attempt {retries + 1})."
                    )
                    time.sleep(RETRY_BASE_WAIT_SECONDS / 2 * (2**retries))
                    processing_queue.put((month_date, retries + 1))
                processing_queue.task_done()
                continue
            except Exception:  # Includes MarkItDown conversion errors
                logger.exception(
                    f"Unexpected error converting {url} ({source_name}, attempt {retries + 1})"
                )
                time.sleep(RETRY_BASE_WAIT_SECONDS / 2 * (2**retries))
                processing_queue.put((month_date, retries + 1))
                processing_queue.task_done()
                continue
        else:
            logger.error(f"Unknown item type in queue: {type(queue_item)}. Skipping.")
            processing_queue.task_done()
            continue

        # --- Process content (common for both local files and URLs) ---
        try:
            if not monthly_raw_markdown.strip():
                logger.warning(f"No content extracted for {source_name}. Skipping further processing.")
                processing_queue.task_done()
                continue

            logger.info(f"Successfully fetched/read content for {source_name}")
            daily_events = split_and_clean_monthly_markdown(monthly_raw_markdown, month_for_processing, logger)

            if not daily_events:
                logger.warning(
                    f"No daily events found or extracted for {source_name}. "
                    "This might be normal for very recent archives or if the content structure changed."
                )

            for event_date, daily_markdown in daily_events:
                logger.info(f"Processing extracted day: {event_date.strftime('%Y-%m-%d')} from {source_name}")
                full_content = generate_jekyll_content(event_date, daily_markdown, logger)
                if full_content and "published: true" in full_content: # Check if content is publishable
                    save_news(event_date, full_content, output_dir, logger)
                else:
                    logger.warning(
                        f"Skipping save for {event_date.strftime('%Y-%m-%d')}: Content marked as unpublished or generation failed."
                    )
        except Exception:
            # This is a general catch-all for errors during the processing part (splitting, saving)
            logger.exception(f"Unexpected error processing content from {source_name}")
        finally:
            processing_queue.task_done()


def main(local_html_files: list[Path] = None, output_dir: str = None) -> None:
    # Set up argument parsing
    parser = argparse.ArgumentParser(description="Download Wikipedia News")
    # Add new argument for local HTML files, not exposed via CLI for now, but used programmatically.
    parser.add_argument("-v", "--verbose", action="store_true", help="Enable verbose logging")
    parser.add_argument(
        "--local-html-dir", # New CLI argument for a directory of local HTML files
        type=Path,
        default=None,
        help="Directory containing local HTML files to process instead of fetching from Wikipedia.",
    )
    parser.add_argument(
        "-o",
        "--output-dir",
        type=str,
        default=DEFAULT_OUTPUT_DIR, # Use constant for default
        help=f"Directory to save markdown files (default: {DEFAULT_OUTPUT_DIR})",
    )
    parser.add_argument(
        "-w",
        "--workers",
        type=int,
        default=None,
        help="Maximum number of concurrent download workers",
    )

    # If local_html_files is provided, we are in programmatic/testing mode for file inputs.
    # Avoid parsing sys.argv which might contain pytest arguments.
    # We'll construct a minimal args object or use defaults for other settings.
    if local_html_files is not None or output_dir is not None: # Programmatic call affecting inputs/outputs
        # Parse with an empty list to use defaults for -v, -w unless also passed programmatically
        # Or, create a Namespace with defaults. For now, empty list is simplest for current need.
        parsed_args_list = []
        # If specific CLI args like --verbose still need to be controllable, they'd need more handling here.
        # For instance, if main got a `verbose_override` param.
        # For this test, we assume default verbosity and worker counts are fine.
        args = parser.parse_args(parsed_args_list)
    else: # Standard CLI execution
        args = parser.parse_args()


    # Setup logging
    # If called programmatically and wanting to control verbosity, main() would need a verbose param.
    # For now, it uses args.verbose which is False if `parsed_args_list` was empty.
    logger = setup_logging(args.verbose)

    # Determine the effective output directory
    # Priority: 1. output_dir param (if provided to main), 2. args.output_dir (CLI or its default)
    current_output_dir = output_dir if output_dir is not None else args.output_dir

    # Ensure output directory exists
    Path(current_output_dir).mkdir(parents=True, exist_ok=True)

    processing_queue: queue.Queue[QueueItem] = queue.Queue()
    items_to_process_count = 0
    operation_mode = "" # For logging clarity

    # Determine if processing local files (either from main() param or CLI)
    # local_html_files param (if provided to main) takes precedence over --local-html-dir CLI arg.
    if local_html_files is not None: # Programmatic list of files
        operation_mode = f"local files provided programmatically ({len(local_html_files)} files)"
        for file_path in local_html_files:
            if file_path.is_file() and file_path.suffix == ".html":
                processing_queue.put(file_path)
            else:
                logger.warning(f"Skipping non-HTML file or directory from local_html_files list: {file_path}")
        items_to_process_count = processing_queue.qsize()
    elif args.local_html_dir:
        operation_mode = f"local files from CLI directory: {args.local_html_dir}"
        cli_local_files = list(Path(args.local_html_dir).glob("*.html"))
        if not cli_local_files: # Should be caught by previous check if args.local_html_dir was used
            logger.warning(f"No HTML files found in specified --local-html-dir: {args.local_html_dir}")
            return
        for file_path in cli_local_files: # Ensure these are Path objects
            processing_queue.put(Path(file_path))
        items_to_process_count = processing_queue.qsize()
    else:
        # Original behavior: Fetch from Wikipedia based on dates
        operation_mode = "Wikipedia URL fetching mode"
        now = datetime.now()
        start_date = datetime(2025, 1, 1)
        end_date = datetime(now.year, now.month, 1)

        dates_set: set[datetime] = set()
        current_date = start_date
        while current_date <= end_date:
            dates_set.add(current_date)
            if current_date.month == 12:
                current_date = datetime(current_date.year + 1, 1, 1)
            else:
                current_date = datetime(current_date.year, current_date.month + 1, 1)

        items_to_process_count = len(dates_set)
        for date_item in sorted(list(dates_set)):
            processing_queue.put((date_item, 0)) # (date, retry_count)

    if items_to_process_count == 0:
        logger.info(f"No items to process in {operation_mode}.")
        return

    logger.info(
        f"Starting processing in mode: {operation_mode}. "
        f"Output directory: {current_output_dir}. "
        f"Using up to {args.workers or min(8, items_to_process_count)} worker thread(s)."
    )

    # Determine num_workers based on args, ensuring it's at least 1 if items_to_process_count > 0
    num_actual_workers = args.workers or min(8, items_to_process_count)
    if items_to_process_count > 0 and num_actual_workers == 0: # min(8,0) could be 0
        num_actual_workers = 1
    elif items_to_process_count == 0: # Ensure no workers if nothing to process
        num_actual_workers = 0


    threads: list[threading.Thread] = []
    for _ in range(num_actual_workers):
        t = threading.Thread(target=worker, args=(processing_queue, current_output_dir, logger))
        t.start()
        threads.append(t)

    if items_to_process_count > 0: # Only join if items were queued
        processing_queue.join()
    for t in threads:
        t.join()

    logger.info("Wikipedia News Download Complete")


if __name__ == "__main__":
    main()
