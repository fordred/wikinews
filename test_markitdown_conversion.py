import logging  # Kept for caplog
from pathlib import Path
from typing import Any  # Added for caplog type hint

import pytest
from markitdown import MarkItDown


@pytest.mark.parametrize(
    ("golden_html_path_str", "golden_reference_md_path_str"),  # PT006
    [
        (
            "tests/golden_html_references/january_2025.html",
            "tests/golden_html_references/january_2025_reference.md",
        ),
        (
            "tests/golden_html_references/february_2025.html",
            "tests/golden_html_references/february_2025_reference.md",
        ),
    ],
)
def test_markitdown_conversion_consistency(
    golden_html_path_str: str,
    golden_reference_md_path_str: str,
    caplog: Any,
) -> None:
    caplog.set_level(logging.DEBUG)  # Capture info logs if any part of MarkItDown uses logging

    golden_html_path = Path(golden_html_path_str)
    golden_reference_md_path = Path(golden_reference_md_path_str)

    assert golden_html_path.exists(), f"Golden HTML file {golden_html_path} does not exist."
    assert golden_reference_md_path.exists(), f"Golden reference markdown file {golden_reference_md_path} does not exist."

    md_converter = MarkItDown()
    current_raw_markdown = ""  # Initialize to ensure it's defined
    try:
        result = md_converter.convert(golden_html_path_str)  # Pass the path string
        if result is None or not hasattr(result, "text_content") or not isinstance(result.text_content, str):
            pytest.fail(
                f"MarkItDown().convert() did not return a result with a valid 'text_content' string attribute "
                f"from {golden_html_path}. Got: {result}",
            )
        current_raw_markdown = result.text_content
    except Exception as e:
        pytest.fail(f"MarkItDown().convert() failed when processing HTML file {golden_html_path}: {e}")

    reference_markdown_content = golden_reference_md_path.read_text(encoding="utf-8")

    # Normalize both markdown strings for comparison: strip leading/trailing whitespace and ensure a single trailing newline.
    normalized_current_md = current_raw_markdown.strip() + "\n"
    normalized_reference_md = reference_markdown_content.strip() + "\n"

    assert normalized_current_md == normalized_reference_md, (
        f"Markdown mismatch between converted HTML ({golden_html_path}) "
        f"and reference MD ({golden_reference_md_path}).\n"
        f"Generated (normalized):\n'''{normalized_current_md}'''\n"
        f"Reference (normalized from file):\n'''{normalized_reference_md}'''"
    )


# Example of how to run this test (assuming pytest is installed):
# python3 -m pytest test_markitdown_conversion.py


def test_markitdown_conversion_detects_mismatch(caplog: Any) -> None:
    caplog.set_level(logging.DEBUG)  # Optional: if you want to log info during this test

    # Use one of the existing golden file pairs
    golden_html_path_str = "tests/golden_html_references/january_2025.html"
    golden_reference_md_path_str = "tests/golden_html_references/january_2025_reference.md"

    golden_html_path = Path(golden_html_path_str)
    golden_reference_md_path = Path(golden_reference_md_path_str)

    assert golden_html_path.exists(), f"Golden HTML file {golden_html_path} does not exist for mismatch test."
    assert golden_reference_md_path.exists(), f"Golden Reference MD file {golden_reference_md_path} does not exist for mismatch test."

    # 1. Get current raw markdown from HTML
    md_converter = MarkItDown()
    current_raw_markdown = ""  # Initialize
    try:
        result = md_converter.convert(golden_html_path_str)
        if result is None or not hasattr(result, "text_content") or not isinstance(result.text_content, str):
            pytest.fail(f"MarkItDown().convert() did not return valid text_content for {golden_html_path_str}")
        current_raw_markdown = result.text_content
    except Exception as e:
        pytest.fail(f"MarkItDown().convert() failed for {golden_html_path_str}: {e}")

    # 2. Read original golden reference markdown
    original_reference_markdown = golden_reference_md_path.read_text(encoding="utf-8")

    # Normalize both for a baseline comparison (optional, but good for sanity)
    normalized_current_raw_md = current_raw_markdown.strip() + "\n"
    normalized_original_reference_md = original_reference_markdown.strip() + "\n"

    # Sanity check: Ensure they are normally equal (this should pass based on the other tests)
    assert normalized_current_raw_md == normalized_original_reference_md, (
        "Baseline comparison failed: the original files do not match as expected."
    )

    # 3. Create a deliberately modified version of the reference markdown
    modified_reference_markdown_content = original_reference_markdown + " deliberate modification"

    # Normalize the modified version
    normalized_modified_reference_md = modified_reference_markdown_content.strip() + "\n"

    # 4. Assert that the comparison between current and MODIFIED reference fails
    with pytest.raises(AssertionError):
        assert normalized_current_raw_md == normalized_modified_reference_md, (
            "AssertionError was expected but not raised for mismatched content."
        )

    # Optional: also test the other way around (modifying current_raw_markdown)
    modified_current_raw_markdown = current_raw_markdown + " another deliberate modification"
    normalized_modified_current_raw_md = modified_current_raw_markdown.strip() + "\n"

    with pytest.raises(AssertionError):
        assert normalized_modified_current_raw_md == normalized_original_reference_md, (
            "AssertionError was expected but not raised when current MD was modified."
        )
