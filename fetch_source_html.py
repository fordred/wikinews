import os
import requests # For making HTTP requests

# --- Configuration ---
OUTPUT_DIR = "tests/test_data/source_html"
URLS_TO_FETCH = {
    "january_2025.html": "https://en.m.wikipedia.org/wiki/Portal:Current_events/January_2025",
    "february_2025.html": "https://en.m.wikipedia.org/wiki/Portal:Current_events/February_2025",
}
# Standard headers to mimic a browser visit, which can sometimes help with access.
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
}
# --- End Configuration ---

def main():
    # Output directory should already exist from the bash command, but make sure.
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    print(f"Starting to fetch HTML source for {len(URLS_TO_FETCH)} pages...")

    for filename, url in URLS_TO_FETCH.items():
        print(f"Fetching HTML from {url} to save as {filename}...")
        try:
            response = requests.get(url, headers=HEADERS, timeout=30) # Added timeout
            response.raise_for_status() # Raise an exception for HTTP errors (4xx or 5xx)

            html_content = response.text # Get the response content as text

            if not html_content:
                print(f"Warning: No HTML content received for {url}. Skipping save for {filename}.")
                continue

            filepath = os.path.join(OUTPUT_DIR, filename)
            with open(filepath, "w", encoding="utf-8") as f:
                f.write(html_content)
            print(f"Successfully saved HTML for {filename} to {filepath} (length: {len(html_content)})")

        except requests.exceptions.RequestException as e:
            print(f"Error fetching {url}: {e}")
        except Exception as e:
            print(f"An unexpected error occurred for {url}: {e}")
            # Continue to next file if one fails

    print("HTML fetching process complete.")

if __name__ == "__main__":
    main()
