# /// script
# requires-python = "==3.13.*"
# dependencies = [
#     "markitdown",
#     "pytz",
# ]
# ///

import argparse
from datetime import datetime, timedelta
import logging
from markitdown import MarkItDown
import os
import pytz
import re
import sys


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
            logging.FileHandler("wikipedia_news_downloader.log", mode="w"),
        ],
    )
    return logging.getLogger(__name__)


def download_wikipedia_news(date, logger):
    """
    Download Wikipedia news for a specific date.
    Returns page content with Jekyll front matter.
    """
    logger.info(f"Attempting to download news for {date}")

    # Wikipedia current events portal URL
    url = f"https://en.m.wikipedia.org/wiki/Portal:Current_events/{date.year}_{date:%B}_{date.day}"
    logger.debug(f"Prepare to page: {url}")

    # Extract markdown text first
    markdown_text = use_markitdown(url, logger)

    # Build front matter
    front_matter_lines = [
        "---",
        "layout: post",
        f"title: {date.strftime('%Y %B %d')}",
        f"date: {date.strftime('%Y-%m-%d')}",
    ]

    # Determine published status based on markdown content
    if markdown_text is None or len(markdown_text) < 10:
        logger.warning(
            f"Markdown text is None or too short ({len(markdown_text) if markdown_text else 'None'}). Setting published: false."
        )
        front_matter_lines.append("published: false")
        content_body = ""  # No content if not published
    else:
        logger.debug(
            f"Markdown text generated. Length: {len(markdown_text)} characters. Setting published: true."
        )
        front_matter_lines.append("published: true")
        content_body = markdown_text
        logger.info(f"Downloaded news for {date}")

    front_matter_lines.append("---")
    front_matter_lines.append("")  # Add a blank line after front matter
    front_matter_lines.append("")  # Add another blank line after front matter

    # Combine front matter and content
    full_content = "\n".join(front_matter_lines) + content_body

    return full_content


def use_markitdown(url, logger, max_retries=5):
    """
    Convert a Wikipedia page to markdown using MarkItDown, with exponential backoff on HTTP 429.
    """
    md = MarkItDown()
    for attempt in range(max_retries):
        try:
            result = md.convert(url)
            logger.debug(f"MarkItDown result: {result.text_content}")
            break
        except Exception as e:
            # Check for HTTP 429 in the exception message
            if (
                hasattr(e, "response")
                and getattr(e.response, "status_code", None) == 429
                or "429" in str(e)
            ):
                wait = 2**attempt
                logger.warning(
                    f"HTTP 429 Too Many Requests for {url}. Waiting {wait}s before retrying (attempt {attempt + 1}/{max_retries})..."
                )
                import time

                time.sleep(wait)
            else:
                logger.error(f"Error fetching {url}: {e}")
                raise
    else:
        logger.error(f"Exceeded max retries for {url}")
        return None

    # Remove text up to and including the line that begins with "[watch]"
    my_text_content = re.sub(
        r"^.*action=watch\)\n", "", result.text_content, flags=re.DOTALL
    )
    # Remove text starting at "[Month" until the end
    my_text_content = re.sub(r"\[Month.*", "", my_text_content, flags=re.DOTALL)
    # Replace relative links with absolute links
    my_text_content = re.sub(
        r"\(/wiki/", r"(https://en.wikipedia.org/wiki/", my_text_content
    )
    # Remove trailing whitespace and newlines
    my_text_content = my_text_content.rstrip()
    my_text_content += "\n"

    logger.debug(f"MarkItDown result head: {my_text_content[:100]}")
    logger.debug(f"MarkItDown result tail: {my_text_content[-100:]}")

    return my_text_content


def save_news(date, markdown_text, logger):
    """
    Save markdown to specified directory.
    """
    logger.info(f"Preparing to save news for {date}")

    # Create date-specific folder
    folder_path = "./docs/_posts/"

    # Create new folder
    os.makedirs(folder_path, exist_ok=True)
    logger.debug(f"Created folder: {folder_path}")

    # Save markdown
    markdown_path = folder_path + date.strftime("%Y-%m-%d") + "-index.md"
    with open(markdown_path, "w", encoding="utf-8", newline="\n") as f:
        f.write(markdown_text)
    logger.info(f"Saved markdown to: {markdown_path}")


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
    args = parser.parse_args()

    # Setup logging
    logger = setup_logging(args.verbose)

    # Dates to download
    nz_timezone = pytz.timezone("Pacific/Auckland")
    nz_time_now = datetime.now(nz_timezone)
    dates = [nz_time_now]

    if args.all:
        logger.info("Downloading all news from Jan 1, 2025, to today")
        start_date = datetime(2025, 1, 1, tzinfo=nz_timezone)
        delta = nz_time_now - start_date
        for i in range(delta.days + 1):
            dates.append(start_date + timedelta(days=i))
    else:
        logger.info("Downloading news for the last 7 days")
        for i in range(1, 7):
            dates.append(nz_time_now - timedelta(days=i))

    logger.info("Starting Wikipedia News Download")

    for date in dates:
        try:
            markdown_text = download_wikipedia_news(date, logger)
            save_news(date, markdown_text, logger)
        except Exception as e:
            logger.error(f"Unexpected error processing {date=}: {e=}")

    logger.info("Wikipedia News Download Complete")


if __name__ == "__main__":
    main()
