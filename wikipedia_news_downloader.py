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
from typing import Any # For type hinting if needed for Queue items before Python 3.9

import requests  # Import requests to catch its exceptions
from markitdown import MarkItDown

# --- Constants ---
# DEFAULT_OUTPUT_DIR will be used by argparse, so it remains global for now.
# LOG_FILE is used by setup_logging, which is called before class instantiation.
DEFAULT_OUTPUT_DIR = "./docs/_posts/"
LOG_FILE = "wikipedia_news_downloader.log"
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


class WikiNewsDownloader:
    BASE_WIKIPEDIA_URL = "https://en.m.wikipedia.org/wiki/Portal:Current_events/"
    RETRY_MAX_ATTEMPTS = 5
    RETRY_BASE_WAIT_SECONDS = 20  # Start wait time for retries
    MIN_MARKDOWN_LENGTH_PUBLISH = 10  # Minimum length to consider content valid for publishing

    # Precompiled regex patterns
    RELATIVE_WIKI_LINK_RE = re.compile(r"\(/wiki/")
    REDLINK_RE = re.compile(r'\[([^\]]+)\]\(/w/index\.php\?title=[^&\s]+&action=edit&redlink=1\s*"[^"]*"\)')
    CITATION_LINK_RE = re.compile(r"\[\[\d+\]\]\(#cite_note-\d+\)")
    TRAILING_SPACES_RE = re.compile(r"[ \t]+$", flags=re.MULTILINE)
    BOLD_HEADINGS_RE = re.compile(r"^\*\*(.*?)\*\*$", flags=re.MULTILINE)
    PLUS_LIST_RE = re.compile(r"^ {2}\+", flags=re.MULTILINE)
    DASH_LIST_RE = re.compile(r"^ {4}\-", flags=re.MULTILINE)
    DAY_DELIMITER_RE = re.compile(
        r"([A-Za-z]+)\xa0(\d{1,2}),\xa0(\d{4})(?:[^\n]*)\n\n"
        r"\* \[edit\]\(.*?\)\n"
        r"\* \[history\]\(.*?\)\n"
        r"\* \[watch\]\(.*?\)\n",
    )
    MONTHLY_MARKDOWN_RE = re.compile(r"\[◀\].*", flags=re.DOTALL)

    MONTH_NAME_TO_NUMBER = {
        "January": 1, "February": 2, "March": 3, "April": 4,
        "May": 5, "June": 6, "July": 7, "August": 8,
        "September": 9, "October": 10, "November": 11, "December": 12,
    }

    def __init__(
        self,
        output_dir: str,
        verbose: bool,
        num_workers: int | None,
        local_html_input_dir: str | None,
        logger: logging.Logger,
        base_url: str = BASE_WIKIPEDIA_URL, # Allow overriding base_url, default to class attr
    ):
        self.output_dir = output_dir
        self.verbose = verbose # Though logger is passed, verbose might be useful for other logic
        self.num_workers = num_workers
        self.local_html_input_dir = local_html_input_dir # Store the string path to the directory
        self.logger = logger
        self.base_wikipedia_url = base_url # Use the passed base_url

        # Ensure output directory exists (moved from global main's start)
        Path(self.output_dir).mkdir(parents=True, exist_ok=True)
        self.logger.info(f"Output directory set to: {self.output_dir}")

    def _clean_daily_markdown_content(self, daily_md: str) -> str:
        """Helper to apply common cleaning rules to daily markdown."""
        # Replace relative links with absolute links
        cleaned_text = self.RELATIVE_WIKI_LINK_RE.sub(r"(https://en.wikipedia.org/wiki/", daily_md)
        # Remove markdown links to non-existent pages (redlinks), leaving only the text
        cleaned_text = self.REDLINK_RE.sub(
            r"\1",
            cleaned_text,
        )
        # Remove any text that looks like [[1]](#cite_note-1)
        cleaned_text = self.CITATION_LINK_RE.sub("", cleaned_text)

        # Remove trailing spaces or tabs from the end of each individual line.
        # This doesn't remove empty lines but cleans lines that only contained spaces/tabs.
        cleaned_text = self.TRAILING_SPACES_RE.sub("", cleaned_text)

        # Turn bold headings into H4 headings.
        cleaned_text = self.BOLD_HEADINGS_RE.sub(r"#### \1", cleaned_text)
        cleaned_text = self.PLUS_LIST_RE.sub("  *", cleaned_text)
        cleaned_text = self.DASH_LIST_RE.sub("    *", cleaned_text)

        # Ensure the entire text block ends with a single newline and has no other trailing whitespace.
        # This also handles cases where the original text might not end with a newline,
        # or collapses multiple trailing newlines from the block into one.
        return cleaned_text.rstrip() + "\n"

    def _generate_jekyll_content(self, date: datetime, markdown_body: str) -> str:
        """Jekyll front matter generator.

        Generates the full page content with Jekyll front matter.
        Returns the full content string, or None if markdown_body is invalid.
        """
        # Determine published status based on markdown content
        is_published = len(markdown_body) >= self.MIN_MARKDOWN_LENGTH_PUBLISH

        if not is_published:
            self.logger.warning(f"Markdown for {date.strftime('%Y-%m-%d')} is too short ({len(markdown_body)=}). Setting published: false.")

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


# Regex to find the start of each day's content block, capturing month name, day, and year.
# Example: June 1, 2025 (2025-06-01) (Sunday) ... [watch]
# \xa0 is a non-breaking space often found in Wikipedia dates.
# Regex to capture Month, Day, Year from a line like:
# June 1, 2025 (2025-06-01) (Sunday)
# followed by lines for edit, history, watch links.
# The content for the day starts AFTER the "* [watch](...)\n" line.
# DAY_DELIMITER_RE moved to class
# MONTHLY_MARKDOWN_RE moved to class

# Note: The original `generate_jekyll_content` was removed in the previous step
# as it was incorporated into the class method `_generate_jekyll_content`.
# Similarly, `clean_daily_markdown_content` became `_clean_daily_markdown_content`.

    def _split_and_clean_monthly_markdown(self, monthly_markdown: str, month_datetime: datetime) -> list[tuple[datetime, str]]:
        """Split markdown by day.

        Splits markdown from a monthly Wikipedia Current Events page into daily segments,
        extracts dates, and cleans each segment.
        """
        # Remove the trailing text that is not part of the daily events.
        monthly_markdown = self.MONTHLY_MARKDOWN_RE.sub("", monthly_markdown)

        daily_events: list[tuple[datetime, str]] = []
        # Find all starting positions of daily segments
        matches = list(self.DAY_DELIMITER_RE.finditer(monthly_markdown))
        self.logger.debug(f"Found {len(matches)} potential daily segments in markdown for {month_datetime.strftime('%Y-%B')}.")

        for i, match in enumerate(matches):
            month_str = day_str = year_str = None  # Ensure variables are always defined
            try:
                month_str, day_str, year_str = match.groups()
                day = int(day_str)
                year = int(year_str)
                month = self.MONTH_NAME_TO_NUMBER.get(month_str)

                if not month:
                    self.logger.warning(f"Could not parse month: {month_str} for segment {i + 1}. Skipping.")
                    continue

                day_dt = datetime(year, month, day)
                self.logger.debug(f"Extracted date for segment {i + 1}: {day_dt.strftime('%Y-%m-%d')}")

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
                cleaned_daily_md = self._clean_daily_markdown_content(daily_raw_content.strip())

                if cleaned_daily_md.strip():  # Ensure there's some content
                    daily_events.append((day_dt, cleaned_daily_md))
                else:
                    self.logger.debug(f"Segment {i + 1} for {day_dt.strftime('%Y-%m-%d')} is empty after cleaning. Skipping.")

            except ValueError:
                self.logger.exception(f"Error parsing date for segment {i + 1} ({month_str} {day_str}, {year_str}). Skipping.")
                continue
            except Exception:
                self.logger.exception(f"Unexpected error processing segment {i + 1} for {month_datetime.strftime('%Y-%B')}")
                continue

        self.logger.info(f"Successfully processed {len(daily_events)} daily segments from {month_datetime.strftime('%Y-%B')}.")
        return daily_events

    def _save_news(self, date: datetime, full_content: str) -> None:
        """Save markdown to specified directory."""
        self.logger.info(f"Preparing to save news for {date}")

        # Output directory is ensured in __init__
        # Path(self.output_dir).mkdir(parents=True, exist_ok=True) # No longer needed here

        # Save markdown
        markdown_path = Path(self.output_dir) / (date.strftime("%Y-%m-%d") + "-index.md")
        with markdown_path.open("w", encoding="utf-8", newline="\n") as f:
            f.write(full_content)
        self.logger.info(f"Saved markdown to: {markdown_path}")

    # (mode, month_datetime, retry_count)
    StructuredQueueItem = tuple[str, datetime, int]

    def _worker( # Corrected indentation for the method definition
        self,
        processing_queue: queue.Queue["WikiNewsDownloader.StructuredQueueItem"], # Use string literal for type hint
        # output_dir, logger, local_html_input_dir are now accessed via self
    ) -> None:
        md_converter = MarkItDown()  # Instantiated once per worker
        while True:
            try:
                mode, month_dt, retries = processing_queue.get(timeout=2)
            except queue.Empty:
                break  # No more items to process

            monthly_raw_markdown = ""
            source_uri: Path | str | None = None  # Initialize source_uri, can be Path or str
            source_name = ""  # For logging (filename or month-year)
            source_name_suffix = month_dt.strftime("%B_%Y")  # e.g., "January_2025"

            if mode == "offline":
                source_name = f"local file for {source_name_suffix}"  # Used for logging
                self.logger.info(f"Processing in offline mode for {month_dt.strftime('%Y-%B')} (retries ignored: {retries})")

                if not self.local_html_input_dir:
                    self.logger.error("Cannot process offline mode: local_html_input_dir not provided to worker.")
                    processing_queue.task_done()
                    continue

                month_name_lower = month_dt.strftime("%B").lower()
                file_name = f"{month_name_lower}_{month_dt.year}.html"
                full_path_obj = Path(self.local_html_input_dir) / file_name

                if full_path_obj.exists():
                    source_uri = full_path_obj  # Store as Path object
                    self.logger.debug(f"Offline mode: Source URI set to path {source_uri}")
                else:
                    self.logger.error(f"Offline mode: Source HTML file not found at {full_path_obj}. Skipping.")
                    processing_queue.task_done()
                    continue

            elif mode == "online":
                source_name = f"online source for {source_name_suffix}"
                self.logger.info(f"Processing in online mode for {month_dt.strftime('%Y-%B')} (attempt {retries + 1})")

                if retries > self.RETRY_MAX_ATTEMPTS:
                    self.logger.error(f"Exceeded max retries ({self.RETRY_MAX_ATTEMPTS}) for {source_name}")
                    processing_queue.task_done()
                    continue

                url = f"{self.base_wikipedia_url}{source_name_suffix}"
                source_uri = url  # Set source_uri to the URL for online mode
                self.logger.debug(f"Online mode: Source URI set to {source_uri}")

            else:  # Should not happen if queue is populated correctly
                self.logger.error(f"Unknown mode in queue item: {mode}. Item: {(mode, month_dt, retries)}. Skipping.")
                processing_queue.task_done()
                continue

            try:
                # md_converter is already instantiated
                self.logger.debug(f"Attempting to convert content from {source_uri} for {source_name}")
                result = md_converter.convert(source_uri)
                monthly_raw_markdown = result.text_content
                self.logger.debug(f"Raw MarkItDown result length for {source_name}: {len(monthly_raw_markdown)}")

                # --- Common content processing (moved inside this try block) ---
                if not monthly_raw_markdown.strip():
                    self.logger.warning(f"No content extracted for {source_name_suffix} (mode: {mode}). Skipping further processing.")
                    continue

                self.logger.info(f"Successfully fetched/read content for {source_name_suffix} (mode: {mode})")

                daily_events = self._split_and_clean_monthly_markdown(monthly_raw_markdown, month_dt)

                if not daily_events:
                    self.logger.warning(f"No daily events found or extracted for {source_name_suffix} (month_dt: {month_dt.strftime('%Y-%B')}).")

                for event_date, daily_md_content in daily_events:
                    self.logger.info(f"Processing extracted day: {event_date.strftime('%Y-%m-%d')} from {source_name_suffix}")
                    full_jekyll_content = self._generate_jekyll_content(event_date, daily_md_content)
                    if full_jekyll_content and "published: true" in full_jekyll_content:
                        self._save_news(event_date, full_jekyll_content)
                    else:
                        self.logger.warning(
                            f"Skipping save for {event_date.strftime('%Y-%m-%d')} from {source_name_suffix}: "
                            "Content marked unpublished or generation failed.",
                        )

            except requests.exceptions.RequestException as e:  # Specific to online mode fetching
                if mode == "online":
                    status_code = getattr(e.response, "status_code", None)
                    url_for_error = source_uri # In online mode, source_uri is the url
                    if status_code == 429:
                        self.logger.warning(f"HTTP 429 Too Many Requests for {url_for_error}. Re-queuing {source_name} (attempt {retries + 1}).")
                        processing_queue.put(("online", month_dt, retries + 1))
                    elif status_code == 404:
                        self.logger.warning(f"HTTP 404 Not Found for {url_for_error} ({source_name}). Skipping.")
                    else:
                        self.logger.warning(f"Request error fetching {url_for_error} ({source_name}): {e}. Retrying (attempt {retries + 1}).")
                        time.sleep(self.RETRY_BASE_WAIT_SECONDS / 2 * (2**retries))
                        processing_queue.put(("online", month_dt, retries + 1))
                else:  # Should not be a RequestException in offline mode if source_uri is Path
                    self.logger.exception(f"Unexpected requests.exceptions.RequestException for {source_uri} in {mode} mode")

            except Exception:  # Catch other errors (MarkItDown conversion, content processing)
                self.logger.exception(
                    f"Error during content conversion or processing for {source_uri} "
                    f"({source_name}, mode: {mode}, attempt {retries if mode == 'online' else 'N/A'})",
                )
                if mode == "online":  # Decide if retry is appropriate for non-network errors in online mode
                    time.sleep(self.RETRY_BASE_WAIT_SECONDS / 2 * (2**retries))
                    processing_queue.put(("online", month_dt, retries + 1))

            finally:  # Ensure task_done is always called once per item from the queue
                processing_queue.task_done()

    def run(self, local_html_files_list: list[Path] | None = None) -> None: # Corrected indentation for the method definition
        """
        Main processing method. Populates queue and manages worker threads.
        """
        processing_queue: queue.Queue[StructuredQueueItem] = queue.Queue()
        items_to_process_count = 0
        operation_mode = ""
        # Determine effective_local_html_input_dir_str for the worker
        # The worker needs a directory path, not a list of files.
        # If local_html_files_list is given, derive from it. Otherwise, use self.local_html_input_dir.
        effective_local_html_input_dir_str: str | None = None
        if local_html_files_list:
            # Ensure list is not empty before accessing parent
            if local_html_files_list[0].parent:
                 effective_local_html_input_dir_str = str(local_html_files_list[0].parent)
        elif self.local_html_input_dir: # Fallback to directory passed during __init__
            effective_local_html_input_dir_str = self.local_html_input_dir


        if local_html_files_list:  # Programmatic list of Path objects takes precedence
            operation_mode = f"local HTML files provided programmatically (input dir: {effective_local_html_input_dir_str})"
            for file_path_obj in local_html_files_list:
                if file_path_obj.is_file() and file_path_obj.suffix == ".html":
                    try:
                        name_parts = file_path_obj.stem.lower().split("_")
                        month_name = name_parts[0].capitalize()
                        year = int(name_parts[1])
                        month_number = self.MONTH_NAME_TO_NUMBER[month_name]
                        month_datetime_obj = datetime(year, month_number, 1)
                        processing_queue.put(("offline", month_datetime_obj, 0))
                    except (IndexError, KeyError, ValueError) as e:
                        self.logger.warning(f"Could not parse valid date from filename {file_path_obj.name}: {e}. Skipping.")
                else:
                    self.logger.warning(f"Skipping non-HTML file or directory from local_html_files input: {file_path_obj}")
            items_to_process_count = processing_queue.qsize()

        if items_to_process_count == 0 and not local_html_files_list: # Online mode
            operation_mode = "Wikipedia URL fetching mode"
            now = datetime.now()
            start_date = datetime(2025, 1, 1) # Consider making this configurable
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
            for date_item in sorted(dates_set):
                processing_queue.put(("online", date_item, 0))

        if items_to_process_count == 0:
            self.logger.info(f"No items to process for mode: {operation_mode}. Exiting.")
            return

        resolved_num_workers = self.num_workers if self.num_workers is not None else min(8, items_to_process_count)
        if items_to_process_count > 0 and resolved_num_workers == 0:
            resolved_num_workers = 1
        elif items_to_process_count == 0: # Should be caught by above, but defensive
            resolved_num_workers = 0

        self.logger.info(
            f"Starting processing in mode: {operation_mode}. "
            f"Output directory: {self.output_dir}. "
            f"Input dir for offline (if applicable): {effective_local_html_input_dir_str}. "
            f"Using up to {resolved_num_workers} worker thread(s).",
        )

        threads: list[threading.Thread] = []
        for _ in range(resolved_num_workers):
            # Note: effective_local_html_input_dir_str is now correctly passed to _worker via self.local_html_input_dir
            # The _worker method uses self.local_html_input_dir which should be set appropriately before calling run,
            # or rely on the value passed to __init__.
            # For the worker, we need to ensure self.local_html_input_dir is the one derived from local_html_files_list if that's the mode.
            # This suggests self.local_html_input_dir might need to be updated within run() if local_html_files_list is used.
            # Or, _worker needs to accept it as a parameter.
            # The plan was for _worker to use self.local_html_input_dir. Let's ensure it's correctly set.
            # If local_html_files_list is used, effective_local_html_input_dir_str should be set to self.local_html_input_dir for the workers.

            # Storing the derived directory path if local_html_files_list was used.
            # This makes it available to self._worker.
            original_local_html_input_dir = self.local_html_input_dir
            if local_html_files_list and effective_local_html_input_dir_str:
                self.local_html_input_dir = effective_local_html_input_dir_str

            t = threading.Thread(target=self._worker, args=(processing_queue,))
            t.start()
            threads.append(t)

            # Restore original if it was changed for this specific run mode
            if local_html_files_list and effective_local_html_input_dir_str:
                 self.local_html_input_dir = original_local_html_input_dir


        if items_to_process_count > 0:
            processing_queue.join()
        for t in threads:
            t.join()

        self.logger.info("Wikipedia News Download Complete")


def parse_arguments(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(description="Download Wikipedia News")
    parser.add_argument("-v", "--verbose", action="store_true", help="Enable verbose logging")
    parser.add_argument(
        "--local-html-dir",
        type=Path,
        default=None,
        help="Directory containing local HTML files to process instead of Wikipedia.",
    )
    parser.add_argument(
        "-o",
        "--output-dir",
        type=str,
        default=DEFAULT_OUTPUT_DIR, # Global constant DEFAULT_OUTPUT_DIR is used here
        help=f"Directory to save markdown files (default: {DEFAULT_OUTPUT_DIR}).",
    )
    parser.add_argument(
        "-w",
        "--workers",
        type=int,
        default=None,
        help="Maximum number of concurrent download workers.",
    )
    return parser.parse_args(argv)


def main() -> None: # Simplified main function
    args = parse_arguments()
    logger = setup_logging(args.verbose)
    html_files_to_pass = None

    # Prepare list of local HTML files if a directory is specified
    if args.local_html_dir:
        local_dir_path = Path(args.local_html_dir)
        if not local_dir_path.is_dir():
            logger.error(f"Provided local HTML directory is not a valid directory: {args.local_html_dir}")
            sys.exit(1)
        html_files_to_pass = list(local_dir_path.glob("*.html"))
        if not html_files_to_pass:
            logger.info(f"No *.html files found in {args.local_html_dir}. Will attempt online mode.")
            # html_files_to_pass will remain None, downloader.run will handle this
        else:
            logger.info(f"Found {len(html_files_to_pass)} HTML file(s) in {args.local_html_dir} to process.")

    # Instantiate the downloader
    downloader = WikiNewsDownloader(
        output_dir=args.output_dir,
        verbose=args.verbose, # verbose is used by setup_logging, but also passed to class
        num_workers=args.workers,
        # Pass the string path of the directory for __init__, run method gets the list of files
        local_html_input_dir=str(args.local_html_dir) if args.local_html_dir else None,
        logger=logger,
        base_url=WikiNewsDownloader.BASE_WIKIPEDIA_URL # Access class attribute for default
    )

    # Run the downloader
    downloader.run(local_html_files_list=html_files_to_pass)


if __name__ == "__main__":
    main()
