import pytest
from pathlib import Path
from markitdown import MarkItDown
# Removed: wikipedia_news_downloader imports, datetime, clean_daily_markdown_content
import logging # Kept for caplog, though might not be strictly necessary for the new logic

# Removed get_markdown_body_from_jekyll_file function

@pytest.mark.parametrize(
    "golden_html_path_str, golden_reference_md_path_str",
    [
        (
            "tests/golden_html_references/january_2025.html",
            "tests/golden_html_references/january_2025_reference.md"
        ),
        (
            "tests/golden_html_references/february_2025.html",
            "tests/golden_html_references/february_2025_reference.md"
        ),
    ]
)
def test_markitdown_conversion_consistency(golden_html_path_str: str, golden_reference_md_path_str: str, caplog):
    caplog.set_level(logging.INFO) # Capture info logs if any part of MarkItDown uses logging

    golden_html_path = Path(golden_html_path_str)
    golden_reference_md_path = Path(golden_reference_md_path_str)

    assert golden_html_path.exists(), f"Golden HTML file {golden_html_path} does not exist."
    assert golden_reference_md_path.exists(), f"Golden reference markdown file {golden_reference_md_path} does not exist."

    md_converter = MarkItDown()
    current_raw_markdown = None
    try:
        result = md_converter.convert(golden_html_path_str) # Pass the path string
        if result is None or not hasattr(result, 'text_content') or not isinstance(result.text_content, str):
             pytest.fail(f"MarkItDown().convert() did not return a result with a valid 'text_content' string attribute from {golden_html_path}. Got: {result}")
        current_raw_markdown = result.text_content
    except Exception as e:
        pytest.fail(f"MarkItDown().convert() failed when processing HTML file {golden_html_path}: {e}")

    reference_markdown_content = golden_reference_md_path.read_text(encoding='utf-8')

    # Normalize both markdown strings for comparison: strip leading/trailing whitespace and ensure a single trailing newline.
    normalized_current_md = current_raw_markdown.strip() + "\n"
    normalized_reference_md = reference_markdown_content.strip() + "\n"

    assert normalized_current_md == normalized_reference_md, \
        f"Markdown mismatch between converted HTML ({golden_html_path}) and reference MD ({golden_reference_md_path}).\n" \
        f"Generated (normalized):\n'''{normalized_current_md}'''\n" \
        f"Reference (normalized from file):\n'''{normalized_reference_md}'''"

# Example of how to run this test (assuming pytest is installed):
# python3 -m pytest test_markitdown_conversion.py
