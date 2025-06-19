import os
from markitdown import MarkItDown

# Create the output directory if it doesn't exist
output_dir = "tests/test_data/offline_pages"
os.makedirs(output_dir, exist_ok=True)

# URLs to fetch
urls = {
    "january_2024.md": "https://en.m.wikipedia.org/wiki/Portal:Current_events/January_2024",
    "february_2024.md": "https://en.m.wikipedia.org/wiki/Portal:Current_events/February_2024",
}

# Instantiate MarkItDown
md = MarkItDown()

# Fetch and save each page
for filename, url in urls.items():
    print(f"Fetching {url}...")
    try:
        # Convert URL to Markdown
        result = md.convert(url)
        markdown_content = result.markdown # Assuming result has a .markdown attribute

        # Save the Markdown content to a file
        filepath = os.path.join(output_dir, filename)
        with open(filepath, "w", encoding="utf-8") as f:
            f.write(markdown_content)
        print(f"Saved {filename} to {filepath}")
    except Exception as e:
        print(f"Error fetching or saving {url}: {e}")

print("Done.")
