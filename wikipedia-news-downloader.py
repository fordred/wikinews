# /// script
# requires-python = "==3.13"
# dependencies = [
#     "beautifulsoup4==4.12.3",
#     "requests==2.32.3",
#     "ruff==0.8.1",
# ]
# ///
import os
import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import shutil
import logging
import argparse
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
        content = soup.find("div", {"id": "mw-content-text"})

        if not content:
            logger.warning(f"No content found for {date}")
            return None

        # Extract markdown text
        markdown_text = convert_to_markdown(content, logger)
        logger.debug(
            f"Markdown text generated. Length: {len(markdown_text)} characters"
        )

        return markdown_text

    except requests.RequestException as e:
        logger.error(f"Could not download news for {date}: {e}")
        return None


def convert_to_markdown(content, logger):
    """
    Convert HTML content to markdown, focusing on the description content.
    """
    # Find the div with the specific class
    logger.info("Converting HTML to markdown")
    description_div = content.find("div", class_="current-events-content description")

    if not description_div:
        logger.error(f"Could not find any text")
        return ""

    # Remove edit section links and other unwanted elements
    for tag in description_div.find_all(
        ["sup", "span", "div"], class_=["mw-editsection"]
    ):
        logger.debug(f"Removing tag: {tag}")
        tag.decompose()

    markdown = []

    # Process bold headers
    for bold_header in description_div.find_all("b"):
        logger.debug(f"Processing bold header: {bold_header.get_text().strip()}")
        markdown.append(f"## {bold_header.get_text().strip()}")

    # Process lists and paragraphs
    for element in description_div.find_all(["p", "ul"]):
        logger.debug(f"Processing element: {element.name}")
        if element.name == "p":
            markdown.append(element.get_text().strip())
        elif element.name == "ul":
            for li in element.find_all("li", recursive=False):
                # Handle nested lists
                if li.find("ul"):
                    # First, add the main list item
                    markdown.append(f"- {li.contents[0].strip()}")
                    # Then add nested list items
                    for nested_li in li.find("ul").find_all("li"):
                        markdown.append(f"  - {nested_li.get_text().strip()}")
                else:
                    markdown.append(f"- {li.get_text().strip()}")

    return "\n\n".join(markdown)


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
    with open(markdown_path, "w", encoding="utf-8") as f:
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
