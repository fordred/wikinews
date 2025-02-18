# /// script
# requires-python = "==3.13"
# dependencies = [
#     "markitdown",
#     "pytz",
#     "ruff",
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
    Returns page content.
    """
    logger.info(f"Attempting to download news for {date}")

    # Wikipedia current events portal URL
    url = f"https://en.m.wikipedia.org/wiki/Portal:Current_events/{date.year}_{date:%B}_{date.day}"
    logger.debug(f"Prepare to page: {url}")

    # Extract markdown text
    front_matter = "---\n"
    front_matter += "layout: post\n"
    front_matter += "title: " + date.strftime("%Y %B %d") + "\n"
    front_matter += "date: " + date.strftime("%Y-%m-%d") + "\n"
    front_matter += "---\n\n"

    markdown_text = use_markitdown(url, logger)
    logger.debug(f"Markdown text generated. Length: {len(markdown_text)} characters")
    if markdown_text is None:
        logger.warning(f"Markdown_text is None")
        return None
    elif len(markdown_text) < 10:
        logger.warning(f"Markdown text length is less than 10. {len(markdown_text)=}")
        return None
    else:
        logger.info(f"Downloaded news for {date}")
        return (front_matter + markdown_text)


def use_markitdown(url, logger):
    logger.debug("MarkItDown: Converting soup to markdown")
    md = MarkItDown()
    result = md.convert(url)
    if "page does not exist" in result.text_content:
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
    args = parser.parse_args()

    # Setup logging
    logger = setup_logging(args.verbose)

    # Dates to download
    nz_timezone = pytz.timezone("Pacific/Auckland")
    nz_time_now = datetime.now(nz_timezone)
    dates = [nz_time_now]
    for i in range(1, 7):
        dates.append(nz_time_now - timedelta(days=i))

    logger.info("Starting Wikipedia News Download")

    for date in dates:
        try:
            markdown_text = download_wikipedia_news(date, logger)
            if markdown_text:
                save_news(date, markdown_text, logger)
            else:
                logger.warning(f"No content available for {date}")
        except Exception as e:
            logger.error(f"Unexpected error processing {date}: {e}")

    logger.info("Wikipedia News Download Complete")


if __name__ == "__main__":
    main()
