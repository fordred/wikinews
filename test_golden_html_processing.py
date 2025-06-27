import logging
import tempfile
from datetime import datetime
from pathlib import Path

import pytest

# Import the refactored main and other necessary components
from wikipedia_news_downloader import WikiNewsDownloader # Import the class
# main is no longer imported directly for calling with specific args in this test

# Pytest will configure the root logger, so we can just get a logger instance.
# The log level can be controlled from the pytest command line.
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


def get_month_year_from_golden_html(html_file_path: Path) -> tuple[int, int]:
    """Derives month and year from golden HTML filename e.g., january_2025.html -> (1, 2025)"""
    parts = html_file_path.stem.lower().split("_")
    month_name = parts[0].capitalize()
    year = int(parts[1])
    # Access MONTH_NAME_TO_NUMBER via the class
    month_number = WikiNewsDownloader.MONTH_NAME_TO_NUMBER[month_name]
    return month_number, year


def test_html_processing_with_refactored_main(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Tests HTML to Jekyll post conversion using the refactored main function
    from wikipedia_news_downloader.py.
    It processes golden HTML files into a temporary directory and then
    compares these generated posts against the reference posts in docs/_posts/.
    """
    golden_html_root_dir = Path("tests/golden_html_references/")
    reference_posts_root_dir = Path("docs/_posts/")

    golden_html_files = list(golden_html_root_dir.glob("*.html"))
    if not golden_html_files:
        pytest.skip("No golden HTML files found in tests/golden_html_references/. Skipping test.")

    # Create a set of (month, year) tuples from the golden HTML files to know what they cover
    covered_months_years: set[tuple[int, int]] = set()
    for html_file in golden_html_files:
        try:
            month_num, year_num = get_month_year_from_golden_html(html_file)
            covered_months_years.add((month_num, year_num))
        except Exception as e:
            pytest.fail(f"Could not parse month/year from golden HTML file {html_file.name}: {e}")

    with tempfile.TemporaryDirectory() as temp_dir_name:
        temp_output_dir = Path(temp_dir_name)

        logger.info(f"Running wikipedia_main with local HTML files: {golden_html_files}")
        logger.info(f"Outputting to temporary directory: {temp_output_dir}")

        try:
            # Instantiate WikiNewsDownloader
            # The local_html_input_dir for __init__ can be derived from the parent of the first golden HTML file,
            # or set to None if run handles it entirely based on local_html_files_list.
            # The `run` method's logic for effective_local_html_input_dir_str will handle this.
            # We provide the parent of the first golden file as the base input dir for consistency.
            input_dir_for_init = str(golden_html_files[0].parent) if golden_html_files else None

            downloader = WikiNewsDownloader(
                output_dir=str(temp_output_dir),
                verbose=False, # Test logger is used, this is for internal consistency if class uses it
                num_workers=1, # Use 1 worker for deterministic testing
                local_html_input_dir=input_dir_for_init,
                logger=logger, # Use the test's logger instance
                # base_url is not strictly needed for local HTML processing but good to provide default
                base_url=WikiNewsDownloader.BASE_WIKIPEDIA_URL
            )
            # Call the run method with the list of local HTML paths
            downloader.run(local_html_files_list=golden_html_files)

        except Exception as e:
            pytest.fail(f"WikiNewsDownloader().run() failed during test execution: {e}")

        # Compare generated files in temp_output_dir with reference files in docs/_posts/
        reference_markdown_files = list(reference_posts_root_dir.glob("*.md"))

        if not reference_markdown_files:
            pytest.skip("No reference markdown files found in docs/_posts/. Skipping comparison.")

        for ref_post_path in reference_markdown_files:
            try:
                # Extract date from reference post filename e.g., "2025-01-15-index.md"
                date_str = ref_post_path.name.rsplit("-index.md", 1)[0]
                event_date = datetime.strptime(date_str, "%Y-%m-%d")
            except ValueError:
                logger.warning(f"Could not parse date from reference file {ref_post_path.name}. Skipping this file.")
                continue

            # Check if this reference file's month/year is covered by any of the golden HTML inputs
            if (event_date.month, event_date.year) not in covered_months_years:
                logger.info(f"Skipping reference file {ref_post_path.name} as its month/year is not covered by golden HTMLs.")
                continue

            generated_post_path = temp_output_dir / ref_post_path.name

            if not generated_post_path.exists():
                # Find which golden HTML was supposed to generate this
                source_html_candidate = f"{event_date.strftime('%B').lower()}_{event_date.year}.html"
                pytest.fail(
                    f"Missing generated file: {generated_post_path.name} in temp output. "
                    f"(Expected from: {source_html_candidate}, Reference: {ref_post_path})",
                )
                continue

            generated_content = generated_post_path.read_text(encoding="utf-8").strip()
            reference_content = ref_post_path.read_text(encoding="utf-8").strip()

            if generated_content != reference_content:
                source_html_candidate = f"{event_date.strftime('%B').lower()}_{event_date.year}.html"
                pytest.fail(
                    f"Content mismatch for {generated_post_path.name}.\n"
                    f"  Source HTML (expected): {source_html_candidate}\n"
                    f"  Reference MD: {ref_post_path}\n"
                    f"  Generated MD: {generated_post_path}\n",
                    # Consider adding a diff here if it's not too verbose, or a marker
                    # For now, full content is not included in assertion to avoid huge outputs
                    # Pytest diffing for strings might handle this well if we directly assert.
                )

        # Also check for any files generated in temp_dir that are NOT in reference_posts_root_dir
        # but correspond to a covered month/year. This might indicate new, untested content.
        for gen_file_path in temp_output_dir.glob("*.md"):
            try:
                date_str = gen_file_path.name.rsplit("-index.md", 1)[0]
                event_date = datetime.strptime(date_str, "%Y-%m-%d")
            except ValueError:
                logger.warning(f"Could not parse date from generated file {gen_file_path.name} in temp dir. Skipping this file.")
                continue

            if (event_date.month, event_date.year) in covered_months_years:
                corresponding_ref_path = reference_posts_root_dir / gen_file_path.name
                if not corresponding_ref_path.exists():
                    pytest.fail(
                        f"Untested generated file: {gen_file_path.name} was created in temp output, "
                        f"but no corresponding reference file exists in {reference_posts_root_dir}.",
                    )


# Remove old test if it exists (or rename it to avoid running both)
# For example, if the old one was test_golden_html_processing_with_workers
# This new one is test_html_processing_with_refactored_main
# pytest will pick up functions starting with "test_"
# Ensure the old test function is removed or renamed if it's in the same file.
# Since we are overwriting the file, the old test function is implicitly removed.
# The `tmp_path` fixture from the old test is not used here as `tempfile` is used directly.
# If `tmp_path` is preferred, `temp_output_dir` can be `tmp_path / "output"`.
# Using `tempfile.TemporaryDirectory()` is also fine.
