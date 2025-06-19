import os
from markitdown import MarkItDown # Assuming MarkItDown is installed

# --- Configuration ---
OUTPUT_DIR = "tests/test_data/offline_pages"
URLS_TO_FETCH = {
    "january_2025.md": "https://en.m.wikipedia.org/wiki/Portal:Current_events/January_2025",
    "february_2025.md": "https://en.m.wikipedia.org/wiki/Portal:Current_events/February_2025",
}
# --- End Configuration ---

def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    md_converter = MarkItDown()

    print(f"Starting to fetch {len(URLS_TO_FETCH)} pages...")

    for filename, url in URLS_TO_FETCH.items():
        print(f"Fetching {url} to save as {filename}...")
        try:
            result = md_converter.convert(url)
            # As per subtask, using .text_content for the main textual content
            markdown_content = result.text_content

            if not markdown_content:
                print(f"Warning: No text_content found for {url}. Skipping save for {filename}.")
                continue

            filepath = os.path.join(OUTPUT_DIR, filename)
            with open(filepath, "w", encoding="utf-8") as f:
                f.write(markdown_content)
            print(f"Successfully saved {filename} to {filepath} (length: {len(markdown_content)})")

        except Exception as e:
            print(f"Error fetching or saving {url}: {e}")
            # Continue to next file if one fails

    print("Fetching process complete.")

if __name__ == "__main__":
    main()
