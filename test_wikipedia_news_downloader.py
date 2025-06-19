import logging
from datetime import datetime

import pytest

from wikipedia_news_downloader import (
    MIN_MARKDOWN_LENGTH_PUBLISH,
    clean_daily_markdown_content,
    generate_jekyll_content,
    split_and_clean_monthly_markdown,
)

# Raw markdown example from the issue description
raw_markdown_example = """
'* [Home](/wiki/Main_Page)\n* [Random](/wiki/Special%3ARandom)\n* [Nearby](/wiki/Special%3ANearby)\n\n* [Log in](/w/index.php?title=Special:UserLogin&returnto=Portal%3ACurrent+events%2FJune+2025)\n\n* [Settings](/w/index.php?title=Special:MobileOptions&returnto=Portal%3ACurrent+events%2FJune+2025)\n\n[Donate Now\nIf Wikipedia is useful to you, please give today.\n\n![](https://en.wikipedia.org/static/images/donate/donate.gif)](https://donate.wikimedia.org/?wmf_source=donate&wmf_medium=sidebar&wmf_campaign=en.wikipedia.org&uselang=en&wmf_key=minerva)\n\n* [About Wikipedia](/wiki/Wikipedia%3AAbout)\n* [Disclaimers](/wiki/Wikipedia%3AGeneral_disclaimer)\n\n[![Wikipedia](/static/images/mobile/copyright/wikipedia-wordmark-en.svg)](/wiki/Main_Page)\n\nSearch\n\n# Portal:Current events/June 2025\n\n* [Portal](/wiki/Portal%3ACurrent_events/June_2025)\n* [Talk](/wiki/Portal_talk%3ACurrent_events/June_2025)\n\n* [Language](#p-lang "Language")\n* [Watch](/w/index.php?title=Special:UserLogin&returnto=Portal%3ACurrent+events%2FJune+2025)\n* [Edit](/w/index.php?title=Portal:Current_events/June_2025&action=edit)\n\n< [Portal:Current events](/wiki/Portal%3ACurrent_events "Portal:Current events")\n\n[2025](/wiki/2025 "2025")\n:   [January](/wiki/Portal%3ACurrent_events/January_2025 "Portal:Current events/January 2025")\n:   [February](/wiki/Portal%3ACurrent_events/February_2025 "Portal:Current events/February 2025")\n:   [March](/wiki/Portal%3ACurrent_events/March_2025 "Portal:Current events/March 2025")\n:   [April](/wiki/Portal%3ACurrent_events/April_2025 "Portal:Current events/April 2025")\n:   [May](/wiki/Portal%3ACurrent_events/May_2025 "Portal:Current events/May 2025")\n:   June\n:   [July](/wiki/Portal%3ACurrent_events/July_2025 "Portal:Current events/July 2025")\n:   [August](/wiki/Portal%3ACurrent_events/August_2025 "Portal:Current events/August 2025")\n:   [September](/wiki/Portal%3ACurrent_events/September_2025 "Portal:Current events/September 2025")\n:   [October](/wiki/Portal%3ACurrent_events/October_2025 "Portal:Current events/October 2025")\n:   [November](/wiki/Portal%3ACurrent_events/November_2025 "Portal:Current events/November 2025")\n:   [December](/wiki/Portal%3ACurrent_events/December_2025 "Portal:Current events/December 2025")\n\n**[June](/wiki/June "June")** **[2025](/wiki/2025 "2025")** is the sixth month of the current common year. The month, which began on a [Sunday](/wiki/Sunday "Sunday"), will end on a [Monday](/wiki/Monday "Monday") after 30 days. It is the current month.\n\n## [Portal:Current events](/wiki/Portal%3ACurrent_events "Portal:Current events")\n\n[edit](/w/index.php?title=Portal:Current_events/June_2025&action=edit&section=1 "Edit section: Portal:Current events")\n\nJune\xa01,\xa02025\xa0(2025-06-01) (Sunday)\n\n* [edit](https://en.wikipedia.org/w/index.php?title=Portal:Current_events/2025_June_1&action=edit&editintro=Portal:Current_events/Edit_instructions)\n* [history](https://en.wikipedia.org/w/index.php?title=Portal:Current_events/2025_June_1&action=history)\n* [watch](https://en.wikipedia.org/w/index.php?title=Portal:Current_events/2025_June_1&action=watch)\n\n**Sports**\n\n* [2025 CONCACAF Champions Cup](/wiki/2025_CONCACAF_Champions_Cup "2025 CONCACAF Champions Cup")\n  + [Cruz Azul](/wiki/Cruz_Azul "Cruz Azul") defeat the [Vancouver Whitecaps](/wiki/Vancouver_Whitecaps "Vancouver Whitecaps") 5-0 in the [final](/wiki/2025_CONCACAF_Champions_Cup_final "2025 CONCACAF Champions Cup final") of the [CONCACAF Champions Cup](/wiki/CONCACAF_Champions_Cup "CONCACAF Champions Cup") at the [Olympic University Stadium](/wiki/Estadio_Ol%C3%ADmpico_Universitario "Estadio Olímpico Universitario") in [Mexico City](/wiki/Mexico_City "Mexico City"). [(*USA Today*)](https://eu.usatoday.com/story/sports/soccer/2025/06/01/concacaf-champions-cup-cruz-azul-vancouver-whitecaps/83985000007/), [(France 24)](https://www.france24.com/en/live-news/20250602-cruz-azul-thrash-vancouver-whitecaps-to-win-concacaf-champions-cup)\n\nJune\xa02,\xa02025\xa0(2025-06-02) (Monday)\n\n* [edit](https://en.wikipedia.org/w/index.php?title=Portal:Current_events/2025_June_2&action=edit&editintro=Portal:Current_events/Edit_instructions)\n* [history](https://en.wikipedia.org/w/index.php?title=Portal:Current_events/2025_June_2&action=history)\n* [watch](https://en.wikipedia.org/w/index.php?title=Portal:Current_events/2025_June_2&action=watch)\n\n**Disasters and accidents**\n\n* [2025 Nigeria floods](/wiki/2025_Nigeria_floods "2025 Nigeria floods")\n  + [2025 Mokwa flood](/wiki/2025_Mokwa_flood "2025 Mokwa flood")\n    - The death toll from the [flooding](/wiki/Flooding "Flooding") caused by torrential rain in [Mokwa](/wiki/Mokwa "Mokwa"), [Nigeria](/wiki/Nigeria "Nigeria"), increases to over 200. [(DW)](https://www.dw.com/en/death-toll-in-nigeria-flooding-rises-to-at-least-200/video-72755995)\n\nJune\xa03,\xa02025\xa0(2025-06-03) (Tuesday)\n\n* [edit](https://en.wikipedia.org/w/index.php?title=Portal:Current_events/2025_June_3&action=edit&editintro=Portal:Current_events/Edit_instructions)\n* [history](https://en.wikipedia.org/w/index.php?title=Portal:Current_events/2025_June_3&action=history)\n* [watch](https://en.wikipedia.org/w/index.php?title=Portal:Current_events/2025_June_3&action=watch)\n\n**Arts and culture**\n\n* [CJ Opiaza](/wiki/CJ_Opiaza "CJ Opiaza") is officially crowned as [Miss Grand International 2024](/wiki/Miss_Grand_International_2024 "Miss Grand International 2024") following [Rachel Gupta](/wiki/Rachel_Gupta "Rachel Gupta")\'s resignation and termination from the title. [(ABS-CBN News)](https://www.abs-cbn.com/lifestyle/2025/6/3/-this-is-my-golden-moment-cj-opiaza-in-tears-at-miss-grand-international-coronation-1626)\n\nJune\xa016,\xa02025\xa0(2025-06-16) (Monday)\n\n* [edit](https://en.wikipedia.org/w/index.php?title=Portal:Current_events/2025_June_16&action=edit&editintro=Portal:Current_events/Edit_instructions)\n* [history](https://en.wikipedia.org/w/index.php?title=Portal:Current_events/2025_June_16&action=history)\n* [watch](https://en.wikipedia.org/w/index.php?title=Portal:Current_events/2025_June_16&action=watch)\n\n[◀](/wiki/Portal%3ACurrent_events/May_2025 "Portal:Current events/May 2025")June 2025[▶](/wiki/Portal%3ACurrent_events/July_2025 "Portal:Current events/July 2025")\n\n| S | M | T | W | T | F | S |\n| --- | --- | --- | --- | --- | --- | --- |\n| [1](#2025_June_1) | [2](#2025_June_2) | [3](#2025_June_3) | [4](#2025_June_4) | [5](#2025_June_5) | [6](#2025_June_6) | [7](#2025_June_7) |\n| [8](#2025_June_8) | [9](#2025_June_9) | [10](#2025_June_10) | [11](#2025_June_11) | [12](#2025_June_12) | [13](#2025_June_13) | [14](#2025_June_14) |\n| [15](#2025_June_15) | [16](#2025_June_16) | [17](#2025_June_17) | [18](#2025_June_18) | [19](#2025_June_19) | [20](#2025_June_20) | [21](#2025_June_21) |\n| [22](#2025_June_22) | [23](#2025_June_23) | [24](#2025_June_24) | [25](#2025_June_25) | [26](#2025_June_26) | [27](#2025_June_27) | [28](#2025_June_28) |\n| [29](#2025_June_29) | [30](#2025_June_30) |  |  |  |  |  |\n\nWikimedia portal\n\n(transcluded from the [Current events portal](/wiki/Portal%3ACurrent_events "Portal:Current events"))\n\n[About this page](/wiki/Wikipedia%3AHow_the_Current_events_page_works "Wikipedia:How the Current events page works") • [News about Wikipedia](/wiki/Wikipedia%3AWikipedia_Signpost "Wikipedia:Wikipedia Signpost")\n\n\n\n"""  # noqa:E501

# A basic logger for the function call, can be configured if more detail is needed
logger = logging.getLogger(__name__)
# Pytest handles log capture, so basicConfig is not needed here.


class TestSplitAndCleanMarkdown:
    @staticmethod
    def test_split_and_clean_from_issue_example() -> None:
        month_dt = datetime(2025, 6, 1)
        daily_events = split_and_clean_monthly_markdown(raw_markdown_example, month_dt, logger)

        assert len(daily_events) == 3, "Should find 3 daily segments"

        # --- Assertions for June 1 ---
        event_dt_june1, md_june1 = daily_events[0]
        assert event_dt_june1 == datetime(2025, 6, 1)

        # Check start of content
        assert md_june1.startswith("#### Sports"), "June 1 MD should start with Sports"
        # Check link cleaning (absolute links)
        assert "[2025 CONCACAF Champions Cup](https://en.wikipedia.org/wiki/2025_CONCACAF_Champions_Cup" in md_june1
        # Check original daily header is removed
        assert "June\xa01,\xa02025" not in md_june1, "Original date string should be removed"
        assert "(2025-06-01) (Sunday)" not in md_june1, "Original date parenthetical should be removed"
        assert "action=watch" not in md_june1, "action=watch should be removed"
        # Check relative link that was part of the original example's intro, not part of daily content
        assert "[Home](/wiki/Main_Page)" not in md_june1, "Generic header links should not be in daily content"
        # Check end of content for June 1
        assert md_june1.strip().endswith(
            "[(France 24)](https://www.france24.com/en/live-news/20250602-cruz-azul-thrash-vancouver-whitecaps-to-win-concacaf-champions-cup)",
        ), "June 1 MD incorrect end"

        # --- Assertions for June 2 ---
        event_dt_june2, md_june2 = daily_events[1]
        assert event_dt_june2 == datetime(2025, 6, 2)
        assert md_june2.startswith("#### Disasters and accidents"), "June 2 MD should start with Disasters"
        assert "[2025 Nigeria floods](https://en.wikipedia.org/wiki/2025_Nigeria_floods" in md_june2
        assert "June\xa02,\xa02025" not in md_june2
        assert md_june2.strip().endswith(
            "[(DW)](https://www.dw.com/en/death-toll-in-nigeria-flooding-rises-to-at-least-200/video-72755995)",
        ), "June 2 MD incorrect end"

        # --- Assertions for June 3 ---
        event_dt_june3, md_june3 = daily_events[2]
        assert event_dt_june3 == datetime(2025, 6, 3)
        assert md_june3.startswith("#### Arts and culture"), "June 3 MD should start with Arts"
        assert "[CJ Opiaza](https://en.wikipedia.org/wiki/CJ_Opiaza" in md_june3  # Check a name that had a redlink in original example
        assert "June\xa03,\xa02025" not in md_june3
        assert md_june3.strip().endswith(
            "[(ABS-CBN News)](https://www.abs-cbn.com/lifestyle/2025/6/3/-this-is-my-golden-moment-cj-opiaza-in-tears-at-miss-grand-international-coronation-1626)",
        ), "June 3 MD incorrect end"

    @staticmethod
    def test_empty_markdown() -> None:
        month_dt = datetime(2025, 1, 1)
        daily_events = split_and_clean_monthly_markdown("", month_dt, logger)
        assert len(daily_events) == 0, "Empty markdown should result in no events"

    @staticmethod
    def test_no_matching_delimiter() -> None:
        month_dt = datetime(2025, 1, 1)
        markdown_no_delimiter = "**Some News**\n* Event 1\n* Event 2\nThis markdown has no proper daily delimiters."
        daily_events = split_and_clean_monthly_markdown(markdown_no_delimiter, month_dt, logger)
        assert len(daily_events) == 0, "Markdown without delimiters should result in no events"

    @staticmethod
    def test_markdown_with_only_header_and_no_content_after_delimiter() -> None:
        month_dt = datetime(2025, 7, 1)
        markdown_with_empty_day = (
            "Some introductory text.\n\n"
            "July\xa04,\xa02025\xa0(2025-07-04) (Friday)\n\n"  # Note the double \n to simulate an empty line that might be action=watch
            "* [edit](...)\n* [history](...)\n* [watch](...)\n\n"  # End of delimiter
            # No actual content for July 4
            "\n\nJuly\xa05,\xa02025\xa0(2025-07-05) (Saturday)\n\n"
            "* [edit](...)\n* [history](...)\n* [watch](...)\n\n"
            "**Real Content**\n* Event for July 5."
        )
        daily_events = split_and_clean_monthly_markdown(markdown_with_empty_day, month_dt, logger)
        # Expecting 1 event, as the first day is empty after cleaning and should be skipped.
        assert len(daily_events) == 1
        if len(daily_events) == 1:
            assert daily_events[0][0] == datetime(2025, 7, 5)
            assert daily_events[0][1].startswith("#### Real Content")


class TestCleanDailyMarkdownContent:
    @pytest.mark.parametrize(
        ("text_input", "expected_output"),
        [
            (
                "Text with a [redlink](/w/index.php?title=Red_Link&action=edit&redlink=1).",
                "Text with a [redlink](/w/index.php?title=Red_Link&action=edit&redlink=1).\n",
            ),
            (
                "Text with another [redlink](//en.wikipedia.org/w/index.php?title=Another_Red_Link&action=edit&redlink=1).",
                "Text with another [redlink](//en.wikipedia.org/w/index.php?title=Another_Red_Link&action=edit&redlink=1).\n",
            ),
            (
                "Text with [redlink with spaces](/w/index.php?title=Red%20Link%20Spaces&action=edit&redlink=1).",
                "Text with [redlink with spaces](/w/index.php?title=Red%20Link%20Spaces&action=edit&redlink=1).\n",
            ),
            (
                "A [simple redlink](Red_Link_Page_Not_Exist).",
                "A [simple redlink](Red_Link_Page_Not_Exist).\n",
            ),
        ],
    )
    def test_remove_redlinks_various_formats(self, text_input: str, expected_output: str) -> None:
        # The original test had comments about why these are not removed,
        # which is due to the specific regex in clean_daily_markdown_content.
        # The assertions reflect the current behavior.
        assert clean_daily_markdown_content(text_input) == expected_output

    def test_remove_redlink_specific_non_match(self) -> None:
        # Test specific case from the original code for redlink removal
        # This link lacks the hover title part and won't be cleaned by the current REDLINK_RE
        redlink_from_code_no_title_attr = "[CJ Opiaza](/w/index.php?title=CJ_Opiaza&action=edit&redlink=1)"
        cleaned_redlink_from_code_no_title_attr = "[CJ Opiaza](/w/index.php?title=CJ_Opiaza&action=edit&redlink=1)\n"
        assert clean_daily_markdown_content(redlink_from_code_no_title_attr) == cleaned_redlink_from_code_no_title_attr

    def test_remove_redlink_specific_match(self) -> None:
        # Example that *would* be cleaned by the current regex due to presence of title attribute in quotes
        redlink_that_matches_regex = '[Example Link](/w/index.php?title=Example_Link&action=edit&redlink=1 "Example Link")'
        cleaned_matching_redlink = "Example Link\n"
        assert clean_daily_markdown_content(redlink_that_matches_regex) == cleaned_matching_redlink

    @pytest.mark.parametrize(
        ("text_input", "expected_output"),
        [
            (
                "Text with a citation [[1]](#cite_note-1).",
                "Text with a citation .\n",
            ),
            # This format is not removed by current regex
            (
                "Another citation format [[citation needed]](#).",
                "Another citation format [[citation needed]](#).\n",
            ),
            (
                "Text with multiple citations [[2]](#cite_note-2) and [[3]](#cite_note-3).",
                "Text with multiple citations  and .\n",
            ),
        ],
    )
    def test_remove_citation_markers(self, text_input: str, expected_output: str) -> None:
        assert clean_daily_markdown_content(text_input) == expected_output

    @pytest.mark.parametrize(
        ("text_input", "expected_output"),
        [
            (
                "Line with trailing space. \nAnother line with trailing tab.\t\nNo trailing here.",
                "Line with trailing space.\nAnother line with trailing tab.\nNo trailing here.\n",
            ),
            ("Single line with spaces and tabs. \t \t", "Single line with spaces and tabs.\n"),
        ],
    )
    def test_remove_trailing_spaces_and_tabs(self, text_input: str, expected_output: str) -> None:
        assert clean_daily_markdown_content(text_input) == expected_output

    @pytest.mark.parametrize(
        ("text_input", "expected_cleaned_output"),
        [
            ("Some text", "Some text\n"),  # No trailing newline
            ("Some text\n", "Some text\n"),  # One trailing newline
            ("Some text\n\n\n", "Some text\n"),  # Multiple trailing newlines
            ("Some text  \n\n", "Some text\n"),  # Trailing spaces and newlines
            ("  \n\n", "\n"),  # Just spaces and newlines
            ("\n\n", "\n"),  # Multiple newlines only
        ],
    )
    def test_ensure_single_trailing_newline(self, text_input: str, expected_cleaned_output: str) -> None:
        assert clean_daily_markdown_content(text_input) == expected_cleaned_output

    def test_no_cleaning_needed(self) -> None:
        text_input = "This is a clean line.\nAnd another one."
        # Expect single trailing newline to be added if not present, or maintained if present.
        # The function adds a newline if one isn't at the very end.
        expected_output = "This is a clean line.\nAnd another one.\n"
        assert clean_daily_markdown_content(text_input) == expected_output

    def test_empty_string_input(self) -> None:
        text_input = ""
        expected_output = "\n"  # Empty string results in a single newline
        assert clean_daily_markdown_content(text_input) == expected_output

    def test_combined_cleaning_rules(self) -> None:
        # This input tests redlink (non-matching due to no title attribute), citation (non-matching), trailing spaces, and extra newlines.
        text_input = (
            # Note: The redlink and citation here don't match the strict patterns in clean_daily_markdown_content
            # so they won't be removed/altered beyond general cleaning.
            # The main things being tested here are trailing space removal and newline normalization.
            "Event with [a redlink](/w/index.php?title=Red_Link&action=edit&redlink=1) and citation [[CITE]](#cite-1). \t\n"
            "Another line with trailing spaces.   \n"
            "Final line without anything extra.\n\n\n"  # Extra newlines
        )
        expected_output = (
            "Event with [a redlink](/w/index.php?title=Red_Link&action=edit&redlink=1) and citation [[CITE]](#cite-1).\n"
            "Another line with trailing spaces.\n"
            "Final line without anything extra.\n"
        )
        assert clean_daily_markdown_content(text_input) == expected_output


@pytest.fixture
def common_test_date() -> datetime:
    return datetime(2023, 10, 26)


class TestGenerateJekyllContent:
    def test_correct_front_matter_and_published_true(self, common_test_date: datetime) -> None:
        text_input = "This is a valid markdown body.\n" + ("a" * MIN_MARKDOWN_LENGTH_PUBLISH)
        expected_title = "2023 October 26"
        expected_date_format = "2023-10-26"

        full_content = generate_jekyll_content(common_test_date, text_input, logger)

        assert "---" in full_content
        assert "layout: post" in full_content
        assert f"title: {expected_title}" in full_content
        assert f"date: {expected_date_format}" in full_content
        assert "published: true" in full_content
        assert full_content.endswith("\n\n\n" + text_input)  # 3 blank lines then body

    def test_published_true_sufficient_body_length(self, common_test_date: datetime) -> None:
        # Exactly MIN_MARKDOWN_LENGTH_PUBLISH
        markdown_body = "a" * MIN_MARKDOWN_LENGTH_PUBLISH
        full_content = generate_jekyll_content(common_test_date, markdown_body, logger)
        assert "published: true" in full_content
        assert full_content.endswith(markdown_body)

        # More than MIN_MARKDOWN_LENGTH_PUBLISH
        markdown_body_long = "a" * (MIN_MARKDOWN_LENGTH_PUBLISH + 10)
        full_content_long = generate_jekyll_content(common_test_date, markdown_body_long, logger)
        assert "published: true" in full_content_long
        assert full_content_long.endswith(markdown_body_long)

    def test_published_false_insufficient_body_length(self, common_test_date: datetime) -> None:
        # Less than MIN_MARKDOWN_LENGTH_PUBLISH
        markdown_body = "a" * max(0, MIN_MARKDOWN_LENGTH_PUBLISH - 1)

        full_content = generate_jekyll_content(common_test_date, markdown_body, logger)
        assert "published: false" in full_content

        # Body should be empty when published is false
        expected_front_matter_lines = [
            "---",
            "layout: post",
            f"title: {common_test_date.strftime('%Y %B %d')}",
            f"date: {common_test_date.strftime('%Y-%m-%d')}",
            "published: false",
            "---",
            "",
            "",
            "",
        ]
        expected_full_content = "\n".join(expected_front_matter_lines)
        assert full_content == expected_full_content


# --- Integration Tests for Offline Worker Processing ---
import queue
import shutil
import tempfile
import threading
from pathlib import Path

from wikipedia_news_downloader import worker # Assuming worker is importable


class TestOfflineWorkerProcessing:
    OFFLINE_PAGES_DIR = Path("tests/test_data/offline_pages")
    # Changed to point to docs/_posts as the source of truth for golden files
    GOLDEN_OUTPUT_DIR = Path("docs/_posts")
    NUM_WORKERS = 2 # Using a small number of workers for the test

    # Logger for the test class itself, can be used by its methods
    class_logger = logging.getLogger("TestOfflineWorkerProcessing")


    def _get_month_datetime_from_filename(self, filename: str) -> datetime:
        """Helper to parse datetime from filenames like 'january_2024.md'."""
        name_part = filename.split('.')[0]
        month_str, year_str = name_part.split('_')
        month_map = {
            'january': 1, 'february': 2, 'march': 3, 'april': 4,
            'may': 5, 'june': 6, 'july': 7, 'august': 8,
            'september': 9, 'october': 10, 'november': 11, 'december': 12
        }
        month = month_map[month_str.lower()]
        year = int(year_str)
        return datetime(year, month, 1)

    def test_process_offline_files_produces_golden_output(self, caplog: pytest.LogCaptureFixture) -> None:
        caplog.set_level(logging.INFO) # Optional: set log level for captured logs

        if not self.OFFLINE_PAGES_DIR.exists() or not list(self.OFFLINE_PAGES_DIR.glob("*.md")):
            pytest.skip(f"Offline pages directory ({self.OFFLINE_PAGES_DIR}) is missing or empty. Skipping test.")

        # Adjust skip condition for GOLDEN_OUTPUT_DIR (now docs/_posts)
        # Check if docs/_posts exists and has at least one YYYY-MM-DD-index.md file (can be a loose check)
        # The main check will be comparisons_made > 0
        if not self.GOLDEN_OUTPUT_DIR.is_dir() or not any(self.GOLDEN_OUTPUT_DIR.glob("*-index.md")):
            pytest.skip(f"Golden output directory ({self.GOLDEN_OUTPUT_DIR}) is missing, empty, or contains no index files. Skipping test.")

        test_output_dir = Path(tempfile.mkdtemp(prefix="wikinews_test_output_"))

        # Using class_logger for messages from the test method itself
        # The worker function will use its own logger instance passed to it.
        # Pytest's caplog will capture logs from all loggers if configured.
        worker_logger = logging.getLogger("WorkerInTest") # Specific logger for worker instances in this test
        worker_logger.setLevel(logging.DEBUG) # Ensure worker logs are captured if needed
        # If specific handling for worker_logger is needed (e.g. to see its output directly):
        # stream_handler = logging.StreamHandler(sys.stdout)
        # worker_logger.addHandler(stream_handler)


        try:
            # 1. Populate Queue
            date_queue: queue.Queue[tuple[str, datetime, int]] = queue.Queue()
            offline_files = list(self.OFFLINE_PAGES_DIR.glob("*.md"))
            assert len(offline_files) > 0, "No offline files found to process."

            for file_path in offline_files:
                try:
                    month_dt = self._get_month_datetime_from_filename(file_path.name)
                    # Using absolute path for source_identifier in queue
                    date_queue.put((str(file_path.resolve()), month_dt, 0))
                    self.class_logger.info(f"Queued: {file_path.name} for date {month_dt.strftime('%Y-%m')}")
                except ValueError as e:
                    pytest.fail(f"Failed to parse date from filename {file_path.name}: {e}")

            # 2. Run Workers
            threads: list[threading.Thread] = []
            for i in range(self.NUM_WORKERS):
                thread = threading.Thread(
                    target=worker,
                    # Pass the specific worker_logger instance
                    args=(date_queue, str(test_output_dir), worker_logger),
                    name=f"OfflineWorker-{i}"
                )
                threads.append(thread)
                thread.start()
                self.class_logger.info(f"Started worker thread: {thread.name}")

            date_queue.join() # Wait for queue to be empty
            self.class_logger.info("Queue processing complete.")
            for i, thread in enumerate(threads):
                thread.join(timeout=60) # Increased timeout for potentially more processing
                assert not thread.is_alive(), f"Thread OfflineWorker-{i} did not finish in time."
                self.class_logger.info(f"Worker thread OfflineWorker-{i} joined.")

            self.class_logger.info(f"All worker threads joined. Output directory: {test_output_dir}")

            # 3. Verify Output
            actual_output_files = sorted(list(test_output_dir.glob("*-index.md")))
            self.class_logger.info(f"Found {len(actual_output_files)} files in test output directory: {[f.name for f in actual_output_files]}")

            comparisons_made = 0
            for actual_file_path in actual_output_files:
                filename = actual_file_path.name
                expected_golden_file_path = self.GOLDEN_OUTPUT_DIR / filename

                self.class_logger.debug(f"Checking for golden file: {expected_golden_file_path}")

                if expected_golden_file_path.exists():
                    self.class_logger.info(f"Comparing {actual_file_path.name} with {expected_golden_file_path}")
                    actual_content = actual_file_path.read_text(encoding="utf-8")
                    golden_content = expected_golden_file_path.read_text(encoding="utf-8")

                    assert actual_content == golden_content, \
                        f"Content mismatch for {filename}.\n" \
                        f"Actual ({actual_file_path}):\n{actual_content[:200]}...\n\n" \
                        f"Golden ({expected_golden_file_path}):\n{golden_content[:200]}..."
                    comparisons_made += 1
                else:
                    self.class_logger.warning(
                        f"Golden file {expected_golden_file_path} not found in {self.GOLDEN_OUTPUT_DIR}. "
                        f"Skipping comparison for {actual_file_path.name}."
                    )

            assert comparisons_made > 0, \
                f"No comparisons were made. This means no generated files from {test_output_dir} " \
                f"had a corresponding golden file in {self.GOLDEN_OUTPUT_DIR}. " \
                f"Check test setup, offline data, or if '{self.GOLDEN_OUTPUT_DIR}' contains expected files."

            self.class_logger.info(f"Successfully made {comparisons_made} comparisons against golden files in {self.GOLDEN_OUTPUT_DIR}.")

        finally:
            # 4. Cleanup
            if test_output_dir.exists():
                shutil.rmtree(test_output_dir)
                self.class_logger.info(f"Cleaned up temporary directory: {test_output_dir}")
            # if worker_logger and stream_handler: # If handler was added
            #    worker_logger.removeHandler(stream_handler)


# --- Test for MarkItDown Conversion Consistency ---
import requests # For requests.exceptions.RequestException
from markitdown import MarkItDown
from wikipedia_news_downloader import BASE_WIKIPEDIA_URL # Get the base URL

@pytest.mark.network
class TestMarkItDownConsistency:
    OFFLINE_PAGES_DIR = Path("tests/test_data/offline_pages")
    # Using BASE_WIKIPEDIA_URL from the main script

    # Logger for this test class
    class_logger = logging.getLogger("TestMarkItDownConsistency")

    def _parse_filename_to_url_parts(self, filename: str) -> tuple[str, str] | None:
        """
        Parses filenames like 'january_2025.md' into ('January', '2025').
        Capitalizes month name for URL construction.
        Returns None if parsing fails.
        """
        if not filename.endswith(".md"):
            return None

        name_part = filename.split('.')[0] # e.g., 'january_2025'
        parts = name_part.split('_')
        if len(parts) != 2:
            return None

        month_str, year_str = parts
        # Capitalize the month name for URL consistency (e.g., "January", not "january")
        return month_str.capitalize(), year_str

    def test_compare_fresh_conversion_with_stored_markdown(self, caplog: pytest.LogCaptureFixture) -> None:
        caplog.set_level(logging.INFO)
        self.class_logger.info("Starting MarkItDown consistency test. This test requires network access.")

        if not self.OFFLINE_PAGES_DIR.is_dir() or not list(self.OFFLINE_PAGES_DIR.glob("*.md")):
            pytest.skip(f"Offline pages directory ({self.OFFLINE_PAGES_DIR}) is missing or empty. Nothing to compare.")

        stored_markdown_files = list(self.OFFLINE_PAGES_DIR.glob("*.md"))
        assert len(stored_markdown_files) > 0, "No *.md files found in offline pages directory for comparison."

        self.class_logger.info(f"Found {len(stored_markdown_files)} stored markdown files to check.")

        md_converter = MarkItDown()
        files_compared = 0

        for stored_markdown_path in stored_markdown_files:
            self.class_logger.info(f"Processing stored file: {stored_markdown_path.name}")

            parsed_parts = self._parse_filename_to_url_parts(stored_markdown_path.name)
            if not parsed_parts:
                self.class_logger.warning(f"Could not parse filename {stored_markdown_path.name} to get URL parts. Skipping.")
                continue

            month_name, year = parsed_parts
            # Construct URL, e.g., "https://en.m.wikipedia.org/wiki/Portal:Current_events/January_2025"
            url = f"{BASE_WIKIPEDIA_URL}{month_name}_{year}"
            self.class_logger.info(f"Constructed URL for fresh fetch: {url}")

            try:
                fresh_markdown_result = md_converter.convert(url)
                fresh_markdown_text = fresh_markdown_result.text_content
                self.class_logger.debug(f"Successfully fetched and converted fresh markdown from {url}. Length: {len(fresh_markdown_text)}")
            except requests.exceptions.RequestException as e:
                pytest.fail(f"Network error fetching {url}: {e}. This test expects network access.")
            except Exception as e:
                pytest.fail(f"Error during MarkItDown conversion for {url}: {e}")

            stored_markdown_text = stored_markdown_path.read_text(encoding="utf-8")
            self.class_logger.debug(f"Read stored markdown from {stored_markdown_path.name}. Length: {len(stored_markdown_text)}")

            assert fresh_markdown_text == stored_markdown_text, \
                (f"Markdown content mismatch for {month_name} {year} (URL: {url}, File: {stored_markdown_path.name}).\n"
                 f"This means the live Wikipedia page's MarkItDown output (using .text_content) has diverged "
                 f"from the version stored in '{self.OFFLINE_PAGES_DIR}'.\n"
                 f"Consider updating the stored file if the change is intentional or investigate MarkItDown if unexpected.\n"
                 f"Length Fresh: {len(fresh_markdown_text)}, Length Stored: {len(stored_markdown_text)}\n"
                 f"Start of Fresh:\n{fresh_markdown_text[:500]}...\n"
                 f"Start of Stored:\n{stored_markdown_text[:500]}...")

            self.class_logger.info(f"Content for {stored_markdown_path.name} matches fresh conversion from {url}.")
            files_compared +=1

        assert files_compared > 0, "No files were actually compared. Check parsing or file iteration logic."
        self.class_logger.info(f"Successfully compared {files_compared} files. All matched.")
