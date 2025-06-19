import os
import logging
from datetime import datetime
# Assuming wikipedia_news_downloader.py is in the same directory or in PYTHONPATH
from wikipedia_news_downloader import split_and_clean_monthly_markdown, generate_jekyll_content

# --- Configuration ---
OFFLINE_PAGES_DIR = "tests/test_data/offline_pages"
GOLDEN_OUTPUT_DIR = "tests/test_data/golden_output"
LOG_LEVEL = logging.INFO

# --- Initialize Logger ---
logger = logging.getLogger(__name__)
logger.setLevel(LOG_LEVEL)
handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

# --- Helper function to determine month_datetime ---
def get_month_datetime_from_filename(filename):
    """
    Extracts month and year from filenames like 'january_2024.md'.
    Returns a datetime object for the first day of that month.
    """
    name_part = filename.split('.')[0] # e.g., 'january_2024'
    month_str, year_str = name_part.split('_') # e.g., 'january', '2024'

    month_map = {
        'january': 1, 'february': 2, 'march': 3, 'april': 4,
        'may': 5, 'june': 6, 'july': 7, 'august': 8,
        'september': 9, 'october': 10, 'november': 11, 'december': 12
    }

    month = month_map.get(month_str.lower())
    year = int(year_str)

    if not month:
        raise ValueError(f"Could not parse month from filename: {filename}")

    return datetime(year, month, 1)

# --- Main Processing Logic ---
def main():
    logger.info("Starting golden file generation...")
    os.makedirs(GOLDEN_OUTPUT_DIR, exist_ok=True)

    offline_files = [f for f in os.listdir(OFFLINE_PAGES_DIR) if f.endswith(".md")]
    if not offline_files:
        logger.warning(f"No offline Markdown files found in {OFFLINE_PAGES_DIR}")
        return

    for md_filename in offline_files:
        logger.info(f"Processing {md_filename}...")
        filepath = os.path.join(OFFLINE_PAGES_DIR, md_filename)

        try:
            with open(filepath, "r", encoding="utf-8") as f:
                monthly_markdown_content = f.read()
        except Exception as e:
            logger.error(f"Could not read file {filepath}: {e}")
            continue

        try:
            month_dt = get_month_datetime_from_filename(md_filename)
            logger.info(f"Determined month_datetime: {month_dt.strftime('%Y-%m')} for {md_filename}")
        except ValueError as e:
            logger.error(e)
            continue

        daily_events = split_and_clean_monthly_markdown(monthly_markdown_content, month_dt, logger)
        logger.info(f"Found {len(daily_events)} daily event(s) in {md_filename}")

        for event_date, daily_markdown in daily_events:
            if not daily_markdown.strip():
                logger.info(f"Skipping empty daily markdown for {event_date.strftime('%Y-%m-%d')}")
                continue

            jekyll_content = generate_jekyll_content(event_date, daily_markdown, logger)

            # Check if content is publishable (contains 'published: true' or is non-empty)
            # Current generate_jekyll_content adds 'published: true' by default if markdown is present.
            # So, checking for non-empty jekyll_content is a good proxy.
            if jekyll_content and "published: true" in jekyll_content:
                output_filename = event_date.strftime("%Y-%m-%d") + "-index.md"
                output_filepath = os.path.join(GOLDEN_OUTPUT_DIR, output_filename)

                try:
                    with open(output_filepath, "w", encoding="utf-8") as f:
                        f.write(jekyll_content)
                    logger.info(f"Successfully wrote golden file: {output_filepath}")
                except Exception as e:
                    logger.error(f"Could not write golden file {output_filepath}: {e}")
            else:
                logger.info(f"Skipping non-publishable or empty content for {event_date.strftime('%Y-%m-%d')}")

    logger.info("Golden file generation complete.")

if __name__ == "__main__":
    main()
