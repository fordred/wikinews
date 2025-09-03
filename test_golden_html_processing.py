import logging
import tempfile
from datetime import datetime
from pathlib import Path

import pytest

# Import the refactored main and other necessary components
from wikipedia_news_downloader import (
    MONTH_NAME_TO_NUMBER,
    parse_jekyll_post,
)
from wikipedia_news_downloader import (
    main as wikipedia_main,
)

# Pytest will configure the root logger, so we can just get a logger instance.
# The log level can be controlled from the pytest command line.
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


def get_month_year_from_golden_html(html_file_path: Path) -> tuple[int, int]:
    """Derives month and year from golden HTML filename e.g., january_2025.html -> (1, 2025)"""
    parts = html_file_path.stem.lower().split("_")
    month_name = parts[0].capitalize()
    year = int(parts[1])
    month_number = MONTH_NAME_TO_NUMBER[month_name]
    return month_number, year


def test_html_processing_with_refactored_main() -> None:
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

    # For debugging, it can be useful to prevent the temporary directory from being deleted
    # by passing `delete=False` to TemporaryDirectory (available in Python 3.12+).
    # However, for automated tests, we want the cleanup to happen.
    with tempfile.TemporaryDirectory() as temp_dir_name:
        temp_output_dir = Path(temp_dir_name)

        logger.info(f"Running wikipedia_main with local HTML files: {golden_html_files}")
        logger.info(f"Outputting to temporary directory: {temp_output_dir}")

        # Call the refactored main function with the list of local HTML paths
        # The wikipedia_main function will use its own arg parsing for other params like verbosity
        # We pass verbose=False here if we don't want its logger to override pytest's capture.
        # However, our `main` doesn't take verbose directly, it uses args.verbose.
        # For simplicity in this call, we rely on the default setup_logging or one from `args` if we mocked them.
        # The key is `local_html_files` and `output_dir`.
        try:
            # To control verbosity or other args if necessary, one might need to mock sys.argv
            # or adjust main to accept more direct params. For now, using its CLI defaults for other args.
            # The refactored `main` uses its own `setup_logging` based on its `args.verbose`.
            # We pass the `local_html_files` list directly.
            wikipedia_main(
                output_dir_str=str(temp_output_dir),
                verbose=False,  # Tests typically don't need verbose output from the script itself
                num_workers=None,  # Use default worker logic (or 1 for deterministic testing if issues arise)
                local_html_files_list=golden_html_files,
                logger=logger,
            )
        except Exception as e:
            pytest.fail(f"Call to wikipedia_main failed during test execution: {e}")

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

            generated_content_full = generated_post_path.read_text(encoding="utf-8")
            reference_content_full = ref_post_path.read_text(encoding="utf-8")

            generated_fm, generated_body = parse_jekyll_post(generated_content_full)
            reference_fm, reference_body = parse_jekyll_post(reference_content_full)

            # Remove last_modified_at from generated front matter for comparison
            if "last_modified_at" in generated_fm:
                del generated_fm["last_modified_at"]

            if generated_fm != reference_fm or generated_body.strip() != reference_body.strip():
                source_html_candidate = f"{event_date.strftime('%B').lower()}_{event_date.year}.html"
                pytest.fail(
                    f"Content mismatch for {generated_post_path.name}.\n"
                    f"  Source HTML (expected): {source_html_candidate}\n"
                    f"  Reference MD: {ref_post_path}\n"
                    f"  Generated MD: {generated_post_path}\n",
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
