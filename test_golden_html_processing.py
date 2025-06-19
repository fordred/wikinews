import logging
import queue
import threading
from pathlib import Path
from datetime import datetime # Moved from bottom

import pytest
from markitdown import MarkItDown

from wikipedia_news_downloader import (
    MONTH_NAME_TO_NUMBER,  # Moved from inside worker_process_html
    generate_jekyll_content,
    setup_logging,
    split_and_clean_monthly_markdown,
)

# Setup logger for the test module
logger = setup_logging(verbose=True) # Or False, depending on desired verbosity for tests

def worker_process_html(
    file_queue: queue.Queue[Path],
    golden_references_dir: Path,
    output_posts_dir: Path,
    test_errors: list[str],
):
    """Worker function to process an HTML file and compare with its reference."""
    while True:
        try:
            html_file_path = file_queue.get_nowait()
        except queue.Empty:
            break

        try:
            logger.info(f"Processing {html_file_path.name}")
            md_converter = MarkItDown()
            # Convert HTML file to markdown
            # Assuming MarkItDown().convert() can take a file path string or a Path object.
            # If it needs a URL, this part needs adjustment (e.g., html_file_path.as_uri())
            conversion_result = md_converter.convert(f"file://{html_file_path.resolve()}")
            monthly_markdown_content = conversion_result.text_content

            # The month_datetime is needed for split_and_clean_monthly_markdown.
            # We derive it from the filename, e.g., "january_2025.html"
            parts = html_file_path.stem.split("_")
            month_name = parts[0].capitalize()
            year = int(parts[1])

            # Convert month name to month number
            month_number = MONTH_NAME_TO_NUMBER[month_name]
            month_datetime = datetime(year, month_number, 1)

            daily_events = split_and_clean_monthly_markdown(monthly_markdown_content, month_datetime, logger)

            if not daily_events:
                test_errors.append(f"No daily events extracted from {html_file_path.name}")
                file_queue.task_done()
                continue

            for event_date, daily_md in daily_events:
                generated_content = generate_jekyll_content(event_date, daily_md, logger)

                # Construct the expected reference filename
                reference_filename = f"{event_date.strftime('%Y-%m-%d')}-index.md"
                reference_file_path = output_posts_dir / reference_filename

                if not reference_file_path.exists():
                    test_errors.append(
                        f"Reference file not found for {html_file_path.name} -> {reference_filename} at {reference_file_path}"
                    )
                    continue

                with open(reference_file_path, "r", encoding="utf-8") as ref_file:
                    reference_content = ref_file.read()

                # Normalize content for comparison (e.g., strip trailing newlines)
                normalized_generated_content = generated_content.strip()
                normalized_reference_content = reference_content.strip()

                if normalized_generated_content != normalized_reference_content:
                    test_errors.append(
                        f"Content mismatch for {reference_filename} from {html_file_path.name}.\n"
                        f"Generated:\n'''{generated_content}'''\n"
                        f"Reference:\n'''{reference_content}'''"
                    )
        except Exception as e:
            test_errors.append(f"Error processing {html_file_path.name}: {e}")
        finally:
            file_queue.task_done()

def test_golden_html_processing_with_workers(tmp_path):
    """
    Tests the HTML to Jekyll post conversion against golden reference files.
    """
    golden_html_dir = Path("tests/golden_html_references/")
    # Correctly point to the reference Jekyll posts within the 'docs/_posts' directory
    reference_posts_dir = Path("docs/_posts/")

    html_files = list(golden_html_dir.glob("*.html"))
    if not html_files:
        pytest.skip("No golden HTML files found in tests/golden_html_references/")

    file_q: queue.Queue[Path] = queue.Queue()
    for html_file in html_files:
        file_q.put(html_file)

    test_errors: list[str] = []
    threads: list[threading.Thread] = []
    num_workers = min(4, len(html_files)) # Adjust number of workers as needed

    for _ in range(num_workers):
        thread = threading.Thread(
            target=worker_process_html,
            args=(file_q, golden_html_dir, reference_posts_dir, test_errors),
        )
        thread.start()
        threads.append(thread)

    for thread in threads:
        thread.join() # Wait for threads to finish their current task processing

    file_q.join() # Wait for all items in the queue to be processed

    if test_errors:
        # Collate all errors for a comprehensive failure message
        error_summary = "\n\n".join(test_errors)
        detailed_error_message = (
            f"Golden HTML processing test failed with {len(test_errors)} error(s):\n{error_summary}"
        )
        logger.error(detailed_error_message) # Keep logging for a persistent record
        pytest.fail(detailed_error_message)

# datetime import moved to the top
