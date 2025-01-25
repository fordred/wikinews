# /// script
# requires-python = "==3.13"
# dependencies = [
#     "beautifulsoup4==4.12.3",
#     "requests==2.32.3",
#     "ruff==0.8.1",
# ]
# ///

from bs4 import BeautifulSoup, NavigableString
from datetime import datetime, timedelta
import argparse
import logging
import os
import re
import requests
import shutil
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
    url = f"https://en.m.wikipedia.org/wiki/Portal:Current_events/{date.strftime('%Y_%B_%#d')}"
    logger.debug(f"Prepare to page: {url}")

    try:
        response = requests.get(url)
        response.raise_for_status()
        logger.debug(f"Successfully retrieved page: {url}")

        soup = BeautifulSoup(response.text, "html.parser")

        # Extract main content
        content = soup.find("div", class_="current-events-content")

        if not content:
            logger.warning(f"No content found for {date}")
            return None

        # Extract markdown text
        markdown_text = f"# {date.strftime('%Y_%B_%#d')}\n\n" + convert_to_markdown(
            content, logger
        )
        logger.debug(
            f"Markdown text generated. Length: {len(markdown_text)} characters"
        )

        return markdown_text

    except requests.RequestException as e:
        logger.error(f"Could not download news for {date}: {e}")
        return None


def convert_to_markdown(content, logger):
    markdown_lines = []

    for element in content.children:
        if element.name == "p" and element.b:
            section_title = element.b.get_text().strip()
            logger.debug(f"Found section: {section_title}")
            markdown_lines.append(f"## {section_title}\n\n")
        elif element.name == "ul":
            process_ul(
                element, 3, markdown_lines, logger
            )  # Start with ### for the first ul after ##

    return "".join(markdown_lines).strip()


def process_ul(ul, depth, markdown_lines, logger):
    logger.debug(f"Processing ul at depth {depth}")
    for li in ul.find_all("li", recursive=False):
        process_li(li, depth, markdown_lines, logger)


def process_li(li, depth, markdown_lines, logger):
    logger.debug(f"Processing li at depth {depth}")
    # Handle section header
    sub_ul = li.find("ul", recursive=False)
    if sub_ul:
        logger.debug(f"Found sub ul at depth {depth}")
        section_title = get_content_li(li, markdown_lines, logger)
        logger.debug(f"Found section link: {section_title}")
        markdown_lines.append("#" * depth + " " + section_title + "\n\n")
        process_ul(sub_ul, depth + 1, markdown_lines, logger)
    else:
        content = get_content_li(li, markdown_lines, logger)
        logger.debug(f"Found content: {content}")
        markdown_lines.append("- " + content + "\n\n")


def get_content_li(li, markdown_lines, logger):
    # Handle bullet point
    text_parts = []
    for content in li.contents:
        logger.debug(f"Processing content: {content}")
        if isinstance(content, NavigableString):
            logger.debug(f"Found navigable string: {content}")
            text_parts.append(content)
        elif content.name == "a":
            logger.debug(f"Found link: {content}")
            text_parts.append(content.get_text())
        # Ignore any other elements like nested ul which should be processed separately
    text = " ".join(text_parts).strip()
    # Clean up multiple spaces
    text = " ".join(text.split())
    # Clean up space before punctuation
    text = re.sub(r"\s+([,.])", r"\1", text)
    return text


def save_news(date, markdown_text, logger):
    """
    Save markdown to specified directory.
    """
    # Create date-specific folder
    folder_path = f"./news/{date.strftime('%Y-%m-%d')}"

    logger.info(f"Preparing to save news for {date}")

    # Remove existing folder if it exists
    if os.path.exists(folder_path):
        logger.debug(f"Removing existing folder: {folder_path}")
        shutil.rmtree(folder_path)

    # Create new folder
    os.makedirs(folder_path, exist_ok=True)
    logger.debug(f"Created folder: {folder_path}")

    # Save markdown
    markdown_path = f"{folder_path}/news.md"
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
    dates = [
        # datetime.now().date(),
        datetime.now().date() - timedelta(days=1),
        # datetime.now().date() - timedelta(days=2),
    ]

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
