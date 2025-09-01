#!/usr/bin/env -S uv run
# ruff: noqa: T201
import pathlib
import sys

import requests
from markitdown import MarkItDown  # Added import

URLS = [
    "https://en.m.wikipedia.org/wiki/Portal:Current_events/January_2025",
    "https://en.m.wikipedia.org/wiki/Portal:Current_events/February_2025",
]
OUTPUT_DIR = pathlib.Path("tests/golden_html_references/")


def download_and_save_html(url: str, output_dir: pathlib.Path) -> None:
    """Downloads HTML content from a URL and saves it to a file,
       then converts the HTML to markdown and saves it.

    Args:
        url: The URL to download HTML from.
        output_dir: The directory to save the HTML and markdown files in.
    """
    html_filepath = None  # Initialize to ensure it's defined for error messages
    md_filepath = None  # Initialize
    try:
        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36"
            )
        }
        response = requests.get(url, timeout=10, headers=headers)
        response.raise_for_status()

        # Simplified filename extraction for HTML
        base_filename = url.split("/")[-1].lower()
        if "portal:current_events" in base_filename:  # january_2025, february_2025 etc.
            base_filename = base_filename.replace("portal:current_events_", "")

        html_filename = base_filename + ".html"  # e.g., january_2025.html
        md_filename = base_filename + "_reference.md"  # e.g., january_2025_reference.md

        html_filepath = output_dir / html_filename
        md_filepath = output_dir / md_filename

        # Save the HTML content to a file
        with html_filepath.open("w", encoding="utf-8") as f:
            f.write(response.text)
        print(f"Successfully downloaded HTML to {html_filepath}")
        print(f"Reference markdown will be saved to {md_filepath}")

        # Convert HTML to Markdown and save
        try:
            md_converter = MarkItDown()
            # Pass the path to the downloaded HTML file as a string
            result = md_converter.convert(html_filepath)

            if not hasattr(result, "text_content"):
                print(f"Error: MarkItDown().convert() did not return a valid 'text_content' string for {html_filepath}", file=sys.stderr)
            else:
                with md_filepath.open("w", encoding="utf-8") as f:
                    f.write(result.text_content)
                print(f"Successfully converted HTML and saved reference markdown to {md_filepath}")

        except Exception as e_md:
            print(f"Error converting HTML to Markdown for {html_filepath} or saving to {md_filepath}: {e_md}", file=sys.stderr)

    except requests.exceptions.RequestException as e_req:
        print(f"Error downloading {url}: {e_req}", file=sys.stderr)
    except OSError as e_io:
        if html_filepath:  # Check if html_filepath was assigned
            print(f"Error saving HTML for {url} to {html_filepath}: {e_io}", file=sys.stderr)
        else:
            print(f"Error saving HTML for {url} (filepath not determined): {e_io}", file=sys.stderr)


if __name__ == "__main__":
    try:
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    except OSError as e:
        print(f"Error creating output directory {OUTPUT_DIR}: {e}", file=sys.stderr)
        sys.exit(1)

    for url in URLS:
        download_and_save_html(url, OUTPUT_DIR)
