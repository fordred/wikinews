import logging
import queue  # For queue.Queue and queue.Empty
from datetime import datetime
from pathlib import Path  # For Path object
from typing import Any  # For fixture type hints
from unittest.mock import MagicMock, patch  # For mocking MarkItDown conversion result and patching

import pytest
import requests  # For requests.exceptions.RequestException

from wikipedia_news_downloader import WikiNewsDownloader # Import the class

# Raw markdown example from the issue description
raw_markdown_example = """
'* [Home](/wiki/Main_Page)\n* [Random](/wiki/Special%3ARandom)\n* [Nearby](/wiki/Special%3ANearby)\n\n* [Log in](/w/index.php?title=Special:UserLogin&returnto=Portal%3ACurrent+events%2FJune+2025)\n\n* [Settings](/w/index.php?title=Special:MobileOptions&returnto=Portal%3ACurrent+events%2FJune+2025)\n\n[Donate Now\nIf Wikipedia is useful to you, please give today.\n\n![](https://en.wikipedia.org/static/images/donate/donate.gif)](https://donate.wikimedia.org/?wmf_source=donate&wmf_medium=sidebar&wmf_campaign=en.wikipedia.org&uselang=en&wmf_key=minerva)\n\n* [About Wikipedia](/wiki/Wikipedia%3AAbout)\n* [Disclaimers](/wiki/Wikipedia%3AGeneral_disclaimer)\n\n[![Wikipedia](/static/images/mobile/copyright/wikipedia-wordmark-en.svg)](/wiki/Main_Page)\n\nSearch\n\n# Portal:Current events/June 2025\n\n* [Portal](/wiki/Portal%3ACurrent_events/June_2025)\n* [Talk](/wiki/Portal_talk%3ACurrent_events/June_2025)\n\n* [Language](#p-lang "Language")\n* [Watch](/w/index.php?title=Special:UserLogin&returnto=Portal%3ACurrent+events%2FJune+2025)\n* [Edit](/w/index.php?title=Portal:Current_events/June_2025&action=edit)\n\n< [Portal:Current events](/wiki/Portal%3ACurrent_events "Portal:Current events")\n\n[2025](/wiki/2025 "2025")\n:   [January](/wiki/Portal%3ACurrent_events/January_2025 "Portal:Current events/January 2025")\n:   [February](/wiki/Portal%3ACurrent_events/February_2025 "Portal:Current events/February 2025")\n:   [March](/wiki/Portal%3ACurrent_events/March_2025 "Portal:Current events/March 2025")\n:   [April](/wiki/Portal%3ACurrent_events/April_2025 "Portal:Current events/April 2025")\n:   [May](/wiki/Portal%3ACurrent_events/May_2025 "Portal:Current events/May 2025")\n:   June\n:   [July](/wiki/Portal%3ACurrent_events/July_2025 "Portal:Current events/July 2025")\n:   [August](/wiki/Portal%3ACurrent_events/August_2025 "Portal:Current events/August 2025")\n:   [September](/wiki/Portal%3ACurrent_events/September_2025 "Portal:Current events/September 2025")\n:   [October](/wiki/Portal%3ACurrent_events/October_2025 "Portal:Current events/October 2025")\n:   [November](/wiki/Portal%3ACurrent_events/November_2025 "Portal:Current events/November 2025")\n:   [December](/wiki/Portal%3ACurrent_events/December_2025 "Portal:Current events/December 2025")\n\n**[June](/wiki/June "June")** **[2025](/wiki/2025 "2025")** is the sixth month of the current common year. The month, which began on a [Sunday](/wiki/Sunday "Sunday"), will end on a [Monday](/wiki/Monday "Monday") after 30 days. It is the current month.\n\n## [Portal:Current events](/wiki/Portal%3ACurrent_events "Portal:Current events")\n\n[edit](/w/index.php?title=Portal:Current_events/June_2025&action=edit&section=1 "Edit section: Portal:Current events")\n\nJune\xa01,\xa02025\xa0(2025-06-01) (Sunday)\n\n* [edit](https://en.wikipedia.org/w/index.php?title=Portal:Current_events/2025_June_1&action=edit&editintro=Portal:Current_events/Edit_instructions)\n* [history](https://en.wikipedia.org/w/index.php?title=Portal:Current_events/2025_June_1&action=history)\n* [watch](https://en.wikipedia.org/w/index.php?title=Portal:Current_events/2025_June_1&action=watch)\n\n**Sports**\n\n* [2025 CONCACAF Champions Cup](/wiki/2025_CONCACAF_Champions_Cup "2025 CONCACAF Champions Cup")\n  + [Cruz Azul](/wiki/Cruz_Azul "Cruz Azul") defeat the [Vancouver Whitecaps](/wiki/Vancouver_Whitecaps "Vancouver Whitecaps") 5-0 in the [final](/wiki/2025_CONCACAF_Champions_Cup_final "2025 CONCACAF Champions Cup final") of the [CONCACAF Champions Cup](/wiki/CONCACAF_Champions_Cup "CONCACAF Champions Cup") at the [Olympic University Stadium](/wiki/Estadio_Ol%C3%ADmpico_Universitario "Estadio Olímpico Universitario") in [Mexico City](/wiki/Mexico_City "Mexico City"). [(*USA Today*)](https://eu.usatoday.com/story/sports/soccer/2025/06/01/concacaf-champions-cup-cruz-azul-vancouver-whitecaps/83985000007/), [(France 24)](https://www.france24.com/en/live-news/20250602-cruz-azul-thrash-vancouver-whitecaps-to-win-concacaf-champions-cup)\n\nJune\xa02,\xa02025\xa0(2025-06-02) (Monday)\n\n* [edit](https://en.wikipedia.org/w/index.php?title=Portal:Current_events/2025_June_2&action=edit&editintro=Portal:Current_events/Edit_instructions)\n* [history](https://en.wikipedia.org/w/index.php?title=Portal:Current_events/2025_June_2&action=history)\n* [watch](https://en.wikipedia.org/w/index.php?title=Portal:Current_events/2025_June_2&action=watch)\n\n**Disasters and accidents**\n\n* [2025 Nigeria floods](/wiki/2025_Nigeria_floods "2025 Nigeria floods")\n  + [2025 Mokwa flood](/wiki/2025_Mokwa_flood "2025 Mokwa flood")\n    - The death toll from the [flooding](/wiki/Flooding "Flooding") caused by torrential rain in [Mokwa](/wiki/Mokwa "Mokwa"), [Nigeria](/wiki/Nigeria "Nigeria"), increases to over 200. [(DW)](https://www.dw.com/en/death-toll-in-nigeria-flooding-rises-to-at-least-200/video-72755995)\n\nJune\xa03,\xa02025\xa0(2025-06-03) (Tuesday)\n\n* [edit](https://en.wikipedia.org/w/index.php?title=Portal:Current_events/2025_June_3&action=edit&editintro=Portal:Current_events/Edit_instructions)\n* [history](https://en.wikipedia.org/w/index.php?title=Portal:Current_events/2025_June_3&action=history)\n* [watch](https://en.wikipedia.org/w/index.php?title=Portal:Current_events/2025_June_3&action=watch)\n\n**Arts and culture**\n\n* [CJ Opiaza](/wiki/CJ_Opiaza "CJ Opiaza") is officially crowned as [Miss Grand International 2024](/wiki/Miss_Grand_International_2024 "Miss Grand International 2024") following [Rachel Gupta](/wiki/Rachel_Gupta "Rachel Gupta")\'s resignation and termination from the title. [(ABS-CBN News)](https://www.abs-cbn.com/lifestyle/2025/6/3/-this-is-my-golden-moment-cj-opiaza-in-tears-at-miss-grand-international-coronation-1626)\n\nJune\xa016,\xa02025\xa0(2025-06-16) (Monday)\n\n* [edit](https://en.wikipedia.org/w/index.php?title=Portal:Current_events/2025_June_16&action=edit&editintro=Portal:Current_events/Edit_instructions)\n* [history](https://en.wikipedia.org/w/index.php?title=Portal:Current_events/2025_June_16&action=history)\n* [watch](https://en.wikipedia.org/w/index.php?title=Portal:Current_events/2025_June_16&action=watch)\n\n[◀](/wiki/Portal%3ACurrent_events/May_2025 "Portal:Current events/May 2025")June 2025[▶](/wiki/Portal%3ACurrent_events/July_2025 "Portal:Current events/July 2025")\n\n| S | M | T | W | T | F | S |\n| --- | --- | --- | --- | --- | --- | --- |\n| [1](#2025_June_1) | [2](#2025_June_2) | [3](#2025_June_3) | [4](#2025_June_4) | [5](#2025_June_5) | [6](#2025_June_6) | [7](#2025_June_7) |\n| [8](#2025_June_8) | [9](#2025_June_9) | [10](#2025_June_10) | [11](#2025_June_11) | [12](#2025_June_12) | [13](#2025_June_13) | [14](#2025_June_14) |\n| [15](#2025_June_15) | [16](#2025_June_16) | [17](#2025_June_17) | [18](#2025_June_18) | [19](#2025_June_19) | [20](#2025_June_20) | [21](#2025_June_21) |\n| [22](#2025_June_22) | [23](#2025_June_23) | [24](#2025_June_24) | [25](#2025_June_25) | [26](#2025_June_26) | [27](#2025_June_27) | [28](#2025_June_28) |\n| [29](#2025_June_29) | [30](#2025_June_30) |  |  |  |  |  |\n\nWikimedia portal\n\n(transcluded from the [Current events portal](/wiki/Portal%3ACurrent_events "Portal:Current events"))\n\n[About this page](/wiki/Wikipedia%3AHow_the_Current_events_page_works "Wikipedia:How the Current events page works") • [News about Wikipedia](/wiki/Wikipedia%3AWikipedia_Signpost "Wikipedia:Wikipedia Signpost")\n\n\n\n"""  # noqa:E501

# A basic logger for the function call, can be configured if more detail is needed
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
# Pytest handles log capture, so basicConfig is not needed here.


# --- Fixtures for worker tests ---
@pytest.fixture
def mock_logger(mocker: Any) -> Any:
    return mocker.MagicMock(spec=logging.Logger)


@pytest.fixture
def mock_queue(mocker: Any) -> Any:
    # Create a mock queue that can also raise queue.Empty
    q = mocker.MagicMock(spec=queue.Queue)
    # Configure get to raise queue.Empty after all pre-set items are retrieved
    # This will be customized in each test.
    q.get.side_effect = queue.Empty
    return q


@pytest.fixture
def mock_markitdown_converter(mocker: Any) -> Any:
    mock_converter = mocker.MagicMock()
    # Default behavior for convert, can be overridden in tests
    mock_converter.convert.return_value = MagicMock(text_content="Mocked markdown content")
    return mock_converter


@pytest.fixture
def temp_output_dir(tmp_path: Path) -> str:
    # Create a temporary directory for output files
    output_dir = tmp_path / "output"
    output_dir.mkdir()
    return str(output_dir)


@pytest.fixture
def temp_html_input_dir(tmp_path: Path) -> str:
    # Create a temporary directory for local HTML files
    input_dir = tmp_path / "html_input"
    input_dir.mkdir()
    return str(input_dir)


@pytest.fixture
def downloader_instance(mock_logger: MagicMock, temp_output_dir: str) -> WikiNewsDownloader:
    """Provides a WikiNewsDownloader instance for testing methods."""
    return WikiNewsDownloader(
        output_dir=temp_output_dir,
        verbose=False,
        num_workers=1,
        local_html_input_dir=None,
        logger=mock_logger,
        base_url=WikiNewsDownloader.BASE_WIKIPEDIA_URL
    )

# --- End Fixtures ---


class TestSplitAndCleanMarkdown:
    def test_split_and_clean_from_issue_example(self, downloader_instance: WikiNewsDownloader) -> None:
        month_dt = datetime(2025, 6, 1)
        daily_events = downloader_instance._split_and_clean_monthly_markdown(raw_markdown_example, month_dt)

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

    def test_empty_markdown(self, downloader_instance: WikiNewsDownloader) -> None:
        month_dt = datetime(2025, 1, 1)
        daily_events = downloader_instance._split_and_clean_monthly_markdown("", month_dt)
        assert len(daily_events) == 0, "Empty markdown should result in no events"

    def test_no_matching_delimiter(self, downloader_instance: WikiNewsDownloader) -> None:
        month_dt = datetime(2025, 1, 1)
        markdown_no_delimiter = "**Some News**\n* Event 1\n* Event 2\nThis markdown has no proper daily delimiters."
        daily_events = downloader_instance._split_and_clean_monthly_markdown(markdown_no_delimiter, month_dt)
        assert len(daily_events) == 0, "Markdown without delimiters should result in no events"

    def test_markdown_with_only_header_and_no_content_after_delimiter(self, downloader_instance: WikiNewsDownloader) -> None:
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
        daily_events = downloader_instance._split_and_clean_monthly_markdown(markdown_with_empty_day, month_dt)
        # Expecting 1 event, as the first day is empty after cleaning and should be skipped.
        assert len(daily_events) == 1
        if len(daily_events) == 1:
            assert daily_events[0][0] == datetime(2025, 7, 5)
            assert daily_events[0][1].startswith("#### Real Content")

    def test_split_and_clean_malformed_date(self, downloader_instance: WikiNewsDownloader) -> None: # mock_logger removed, uses downloader_instance.logger
        month_dt = datetime(2025, 6, 1)
        markdown_with_malformed_date = (
            "June\xa01,\xa02025\xa0(2025-06-01) (Sunday)\n\n"
            "* [edit](...)\n* [history](...)\n* [watch](...)\n\n"
            "**Valid Content June 1**\n* Event A\n\n"
            # Malformed date: June 31 is invalid and will cause ValueError in datetime constructor
            "June\xa031,\xa02025\xa0(2025-06-31) (BogusDay)\n\n"
            "* [edit](...)\n* [history](...)\n* [watch](...)\n\n"
            "**Content for Malformed Date**\n* Event B\n\n"
            "June\xa02,\xa02025\xa0(2025-06-02) (Monday)\n\n"
            "* [edit](...)\n* [history](...)\n* [watch](...)\n\n"
            "**Valid Content June 2**\n* Event C\n"
        )

        # Call the function under test
        # No try-except block here for ValueError, as the function should handle it
        daily_events = downloader_instance._split_and_clean_monthly_markdown(markdown_with_malformed_date, month_dt)

        # Assert that only valid segments are processed
        assert len(daily_events) == 2, "Should process 2 valid daily segments, skipping the malformed one."

        # Assert that the valid segments are correct
        if len(daily_events) == 2:
            assert daily_events[0][0] == datetime(2025, 6, 1)
            assert daily_events[0][1].strip().startswith("#### Valid Content June 1")
            assert daily_events[1][0] == datetime(2025, 6, 2)
            assert daily_events[1][1].strip().startswith("#### Valid Content June 2")

        # Assert that the logger was called with a warning or error
        # Check if either warning or error was called. The exact method might depend on implementation.
        # We also check that *a* call contains relevant info about the malformed date.
        # Assert that logger.exception was called with a message containing the malformed date.
        # logger.exception() implies error level and exc_info.
        exception_call_found = False
        for call in downloader_instance.logger.exception.call_args_list: # Use downloader_instance.logger
            log_message = call.args[0]
            if "June 31, 2025" in log_message:
                exception_call_found = True
                break

        assert exception_call_found, "logger.exception() should have been called with a message containing 'June 31, 2025'."


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
    def test_remove_redlinks_various_formats(self, text_input: str, expected_output: str, downloader_instance: WikiNewsDownloader) -> None:
        # The original test had comments about why these are not removed,
        # which is due to the specific regex in clean_daily_markdown_content.
        # The assertions reflect the current behavior.
        assert downloader_instance._clean_daily_markdown_content(text_input) == expected_output

    def test_remove_redlink_specific_non_match(self, downloader_instance: WikiNewsDownloader) -> None:
        # Test specific case from the original code for redlink removal
        # This link lacks the hover title part and won't be cleaned by the current REDLINK_RE
        redlink_from_code_no_title_attr = "[CJ Opiaza](/w/index.php?title=CJ_Opiaza&action=edit&redlink=1)"
        cleaned_redlink_from_code_no_title_attr = "[CJ Opiaza](/w/index.php?title=CJ_Opiaza&action=edit&redlink=1)\n"
        assert downloader_instance._clean_daily_markdown_content(redlink_from_code_no_title_attr) == cleaned_redlink_from_code_no_title_attr

    def test_remove_redlink_specific_match(self, downloader_instance: WikiNewsDownloader) -> None:
        # Example that *would* be cleaned by the current regex due to presence of title attribute in quotes
        redlink_that_matches_regex = '[Example Link](/w/index.php?title=Example_Link&action=edit&redlink=1 "Example Link")'
        cleaned_matching_redlink = "Example Link\n"
        assert downloader_instance._clean_daily_markdown_content(redlink_that_matches_regex) == cleaned_matching_redlink

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
    def test_remove_citation_markers(self, text_input: str, expected_output: str, downloader_instance: WikiNewsDownloader) -> None:
        assert downloader_instance._clean_daily_markdown_content(text_input) == expected_output

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
    def test_remove_trailing_spaces_and_tabs(self, text_input: str, expected_output: str, downloader_instance: WikiNewsDownloader) -> None:
        assert downloader_instance._clean_daily_markdown_content(text_input) == expected_output

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
    def test_ensure_single_trailing_newline(self, text_input: str, expected_cleaned_output: str, downloader_instance: WikiNewsDownloader) -> None:
        assert downloader_instance._clean_daily_markdown_content(text_input) == expected_cleaned_output

    def test_no_cleaning_needed(self, downloader_instance: WikiNewsDownloader) -> None:
        text_input = "This is a clean line.\nAnd another one."
        # Expect single trailing newline to be added if not present, or maintained if present.
        # The function adds a newline if one isn't at the very end.
        expected_output = "This is a clean line.\nAnd another one.\n"
        assert downloader_instance._clean_daily_markdown_content(text_input) == expected_output

    def test_empty_string_input(self, downloader_instance: WikiNewsDownloader) -> None:
        text_input = ""
        expected_output = "\n"  # Empty string results in a single newline
        assert downloader_instance._clean_daily_markdown_content(text_input) == expected_output

    def test_combined_cleaning_rules(self, downloader_instance: WikiNewsDownloader) -> None:
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
        assert downloader_instance._clean_daily_markdown_content(text_input) == expected_output


@pytest.fixture
def common_test_date() -> datetime:
    return datetime(2023, 10, 26)


class TestGenerateJekyllContent:
    def test_correct_front_matter_and_published_true(self, common_test_date: datetime, downloader_instance: WikiNewsDownloader) -> None:
        text_input = "This is a valid markdown body.\n" + ("a" * downloader_instance.MIN_MARKDOWN_LENGTH_PUBLISH)
        expected_title = "2023 October 26"
        expected_date_format = "2023-10-26"

        full_content = downloader_instance._generate_jekyll_content(common_test_date, text_input) # Use downloader_instance.logger implicitly

        assert "---" in full_content
        assert "layout: post" in full_content
        assert f"title: {expected_title}" in full_content
        assert f"date: {expected_date_format}" in full_content
        assert "published: true" in full_content
        assert full_content.endswith("\n\n\n" + text_input)  # 3 blank lines then body

    def test_published_true_sufficient_body_length(self, common_test_date: datetime, downloader_instance: WikiNewsDownloader) -> None:
        # Exactly MIN_MARKDOWN_LENGTH_PUBLISH
        markdown_body = "a" * downloader_instance.MIN_MARKDOWN_LENGTH_PUBLISH
        full_content = downloader_instance._generate_jekyll_content(common_test_date, markdown_body)
        assert "published: true" in full_content
        assert full_content.endswith(markdown_body)

        # More than MIN_MARKDOWN_LENGTH_PUBLISH
        markdown_body_long = "a" * (downloader_instance.MIN_MARKDOWN_LENGTH_PUBLISH + 10)
        full_content_long = downloader_instance._generate_jekyll_content(common_test_date, markdown_body_long)
        assert "published: true" in full_content_long
        assert full_content_long.endswith(markdown_body_long)

    def test_published_false_insufficient_body_length(self, common_test_date: datetime, downloader_instance: WikiNewsDownloader) -> None:
        # Less than MIN_MARKDOWN_LENGTH_PUBLISH
        markdown_body = "a" * max(0, downloader_instance.MIN_MARKDOWN_LENGTH_PUBLISH - 1)

        full_content = downloader_instance._generate_jekyll_content(common_test_date, markdown_body)
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


# --- Tests for worker function ---


class TestWorkerFunction:
    def test_offline_mode_valid_html_file(
        self,
        downloader_instance: WikiNewsDownloader, # Use downloader_instance
        mock_queue: MagicMock,
        mock_markitdown_converter: MagicMock,
        temp_html_input_dir: str, # Still need this to create the file
        mocker: Any,
    ) -> None:
        month_dt = datetime(2024, 1, 1)
        file_name = "january_2024.html"
        html_file_path = Path(temp_html_input_dir) / file_name
        with html_file_path.open("w") as f:  # PTH123
            f.write("<html><body>Mock HTML</body></html>")

        mock_queue.get.side_effect = [("offline", month_dt, 0), queue.Empty]

        # Configure downloader_instance for this test
        downloader_instance.local_html_input_dir = temp_html_input_dir
        # logger and output_dir are already set by the fixture

        mocker.patch("wikipedia_news_downloader.MarkItDown", return_value=mock_markitdown_converter)
        # Patch methods on the class
        mocker.patch.object(WikiNewsDownloader, "_split_and_clean_monthly_markdown", return_value=[(datetime(2024, 1, 1), "Cleaned daily content")])
        mocker.patch.object(WikiNewsDownloader, "_generate_jekyll_content", return_value="Jekyll content published: true")
        mock_save_news = mocker.patch.object(WikiNewsDownloader, "_save_news")

        downloader_instance._worker(mock_queue) # Call the method

        mock_markitdown_converter.convert.assert_called_once_with(html_file_path)
        mock_save_news.assert_called_once()
        mock_queue.task_done.assert_called_once()
        downloader_instance.logger.info.assert_any_call(f"Processing in offline mode for {month_dt.strftime('%Y-%B')} (retries ignored: 0)")

    def test_offline_mode_missing_html_file(
        self,
        downloader_instance: WikiNewsDownloader,
        mock_queue: MagicMock,
        mock_markitdown_converter: MagicMock,
        # temp_output_dir is from downloader_instance
        temp_html_input_dir: str, # Need this to set on downloader_instance
        mocker: Any,
    ) -> None:
        month_dt = datetime(2024, 2, 1)  # February
        mock_queue.get.side_effect = [("offline", month_dt, 0), queue.Empty]

        downloader_instance.local_html_input_dir = temp_html_input_dir

        mocker.patch("wikipedia_news_downloader.MarkItDown", return_value=mock_markitdown_converter)
        # Path.exists will default to False for a non-existent file if not mocked

        downloader_instance._worker(mock_queue) # Call the method

        expected_file_path = Path(temp_html_input_dir) / "february_2024.html"
        downloader_instance.logger.error.assert_any_call(f"Offline mode: Source HTML file not found at {expected_file_path}. Skipping.")
        mock_markitdown_converter.convert.assert_not_called()
        mock_queue.task_done.assert_called_once()

    def test_online_mode_successful_fetch(
        self,
        downloader_instance: WikiNewsDownloader,
        mock_queue: MagicMock,
        mock_markitdown_converter: MagicMock,
        mocker: Any,
    ) -> None:
        month_dt = datetime(2024, 3, 1)
        mock_queue.get.side_effect = [("online", month_dt, 0), queue.Empty]

        downloader_instance.local_html_input_dir = None # Ensure it's online mode

        mocker.patch("wikipedia_news_downloader.MarkItDown", return_value=mock_markitdown_converter)
        # Patch methods on the instance or class
        mocker.patch.object(WikiNewsDownloader, "_split_and_clean_monthly_markdown", return_value=[(datetime(2024, 3, 1), "Cleaned daily content")])
        mocker.patch.object(WikiNewsDownloader, "_generate_jekyll_content", return_value="Jekyll content published: true")
        mock_save_news_on_class = mocker.patch.object(WikiNewsDownloader, "_save_news")


        downloader_instance._worker(mock_queue)

        expected_url = f"{downloader_instance.base_wikipedia_url}{month_dt.strftime('%B_%Y')}"
        mock_markitdown_converter.convert.assert_called_once_with(expected_url)
        mock_save_news_on_class.assert_called_once()
        mock_queue.task_done.assert_called_once()
        downloader_instance.logger.info.assert_any_call(f"Processing in online mode for {month_dt.strftime('%Y-%B')} (attempt 1)")

    def test_online_mode_request_exception_404_no_retry(
        self,
        downloader_instance: WikiNewsDownloader,
        mock_queue: MagicMock,
        mock_markitdown_converter: MagicMock,
        mocker: Any,
    ) -> None:
        month_dt = datetime(2024, 4, 1)
        mock_queue.get.side_effect = [("online", month_dt, 0), queue.Empty]
        downloader_instance.local_html_input_dir = None


        # Simulate a 404 error
        response_mock = MagicMock()
        response_mock.status_code = 404
        error = requests.exceptions.RequestException("Not Found", response=response_mock)
        mock_markitdown_converter.convert.side_effect = error

        mocker.patch("wikipedia_news_downloader.MarkItDown", return_value=mock_markitdown_converter)
        mocker.patch("time.sleep")  # Mock time.sleep to avoid delays

        downloader_instance._worker(mock_queue)

        expected_url = f"{downloader_instance.base_wikipedia_url}{month_dt.strftime('%B_%Y')}"
        mock_markitdown_converter.convert.assert_called_once_with(expected_url)
        downloader_instance.logger.warning.assert_any_call(f"HTTP 404 Not Found for {expected_url} (online source for April_2024). Skipping.")
        mock_queue.put.assert_not_called()  # Should not re-queue for 404
        mock_queue.task_done.assert_called_once()

    def test_online_mode_request_exception_429_retries(
        self,
        downloader_instance: WikiNewsDownloader,
        mock_queue: MagicMock,
        mock_markitdown_converter: MagicMock,
        mocker: Any,
    ) -> None:
        month_dt = datetime(2024, 5, 1)
        downloader_instance.local_html_input_dir = None
        mock_queue.get.side_effect = [("online", month_dt, 0), ("online", month_dt, 1), queue.Empty]

        response_mock = MagicMock()
        response_mock.status_code = 429
        error = requests.exceptions.RequestException("Too Many Requests", response=response_mock)

        mock_markitdown_converter.convert.side_effect = [error, MagicMock(text_content="Successful content after retry")]

        mocker.patch("wikipedia_news_downloader.MarkItDown", return_value=mock_markitdown_converter)
        mocker.patch("time.sleep")
        mocker.patch.object(WikiNewsDownloader, "_split_and_clean_monthly_markdown", return_value=[(datetime(2024, 5, 1), "Cleaned daily content")])
        mocker.patch.object(WikiNewsDownloader, "_generate_jekyll_content", return_value="Jekyll content published: true")
        mocker.patch.object(WikiNewsDownloader, "_save_news")

        downloader_instance._worker(mock_queue)

        expected_url = f"{downloader_instance.base_wikipedia_url}{month_dt.strftime('%B_%Y')}"
        assert mock_markitdown_converter.convert.call_count == 2
        mock_markitdown_converter.convert.assert_any_call(expected_url)
        downloader_instance.logger.warning.assert_any_call(
            f"HTTP 429 Too Many Requests for {expected_url}. Re-queuing online source for May_2024 (attempt 1).",
        )
        mock_queue.put.assert_called_once_with(("online", month_dt, 1))
        assert mock_queue.task_done.call_count == 2

    def test_online_mode_generic_request_exception_retries_then_max_out(  # E501 too long
        self,
        downloader_instance: WikiNewsDownloader,
        mock_queue: MagicMock,
        mock_markitdown_converter: MagicMock,
        mocker: Any,
    ) -> None:
        month_dt = datetime(2024, 6, 1)
        downloader_instance.local_html_input_dir = None

        side_effects = [("online", month_dt, i) for i in range(downloader_instance.RETRY_MAX_ATTEMPTS + 2)]
        side_effects.append(queue.Empty)
        mock_queue.get.side_effect = side_effects

        error = requests.exceptions.RequestException("Some generic network error")
        mock_markitdown_converter.convert.side_effect = error

        mocker.patch("wikipedia_news_downloader.MarkItDown", return_value=mock_markitdown_converter)
        mocker.patch("time.sleep")

        downloader_instance._worker(mock_queue)

        expected_url = f"{downloader_instance.base_wikipedia_url}{month_dt.strftime('%B_%Y')}"
        assert mock_markitdown_converter.convert.call_count == downloader_instance.RETRY_MAX_ATTEMPTS + 1

        for i in range(downloader_instance.RETRY_MAX_ATTEMPTS + 1):
            downloader_instance.logger.warning.assert_any_call(
                f"Request error fetching {expected_url} (online source for June_2024): {error}. Retrying (attempt {i + 1}).",
            )

        downloader_instance.logger.error.assert_any_call(f"Exceeded max retries ({downloader_instance.RETRY_MAX_ATTEMPTS}) for online source for June_2024")
        assert mock_queue.put.call_count == downloader_instance.RETRY_MAX_ATTEMPTS + 1
        assert mock_queue.task_done.call_count == downloader_instance.RETRY_MAX_ATTEMPTS + 2

    def test_offline_mode_generic_exception_on_convert(  # E501 too long
        self,
        downloader_instance: WikiNewsDownloader,
        mock_queue: MagicMock,
        mock_markitdown_converter: MagicMock,
        temp_html_input_dir: str,
        mocker: Any,
    ) -> None:
        month_dt = datetime(2024, 7, 1)
        downloader_instance.local_html_input_dir = temp_html_input_dir
        file_name = "july_2024.html"
        html_file_path = Path(temp_html_input_dir) / file_name
        with html_file_path.open("w") as f:
            f.write("<html><body>Mock HTML</body></html>")

        mock_queue.get.side_effect = [("offline", month_dt, 0), queue.Empty]

        error = Exception("Something went wrong during conversion")
        mock_markitdown_converter.convert.side_effect = error

        mocker.patch("wikipedia_news_downloader.MarkItDown", return_value=mock_markitdown_converter)

        downloader_instance._worker(mock_queue)

        mock_markitdown_converter.convert.assert_called_once_with(html_file_path)
        downloader_instance.logger.exception.assert_any_call(
            f"Error during content conversion or processing for {html_file_path} (local file for July_2024, mode: offline, attempt N/A)",
        )
        mock_queue.put.assert_not_called()
        mock_queue.task_done.assert_called_once()

    def test_online_mode_generic_exception_on_convert_retries(  # E501 too long
        self,
        downloader_instance: WikiNewsDownloader,
        mock_queue: MagicMock,
        mock_markitdown_converter: MagicMock,
        mocker: Any,
    ) -> None:
        month_dt = datetime(2024, 8, 1)
        downloader_instance.local_html_input_dir = None
        mock_queue.get.side_effect = [("online", month_dt, 0), ("online", month_dt, 1), queue.Empty]

        error = Exception("Something went wrong during conversion")
        mock_markitdown_converter.convert.side_effect = [error, MagicMock(text_content="Successful content after retry")]

        mocker.patch("wikipedia_news_downloader.MarkItDown", return_value=mock_markitdown_converter)
        mocker.patch("time.sleep")
        mocker.patch.object(WikiNewsDownloader, "_split_and_clean_monthly_markdown", return_value=[(datetime(2024, 8, 1), "Cleaned daily content")])
        mocker.patch.object(WikiNewsDownloader, "_generate_jekyll_content", return_value="Jekyll content published: true")
        mocker.patch.object(WikiNewsDownloader, "_save_news")

        downloader_instance._worker(mock_queue)

        expected_url = f"{downloader_instance.base_wikipedia_url}{month_dt.strftime('%B_%Y')}"
        assert mock_markitdown_converter.convert.call_count == 2
        downloader_instance.logger.exception.assert_any_call(
            f"Error during content conversion or processing for {expected_url} "
            f"(online source for August_2024, mode: online, attempt 0)",
        )
        mock_queue.put.assert_called_once_with(("online", month_dt, 1))
        assert mock_queue.task_done.call_count == 2

    def test_unknown_mode_in_queue(
        self,
        downloader_instance: WikiNewsDownloader,
        mock_queue: MagicMock,
        mocker: Any,
    ) -> None:
        month_dt = datetime(2024, 9, 1)
        mock_queue.get.side_effect = [("unknown_mode", month_dt, 0), queue.Empty]
        downloader_instance.local_html_input_dir = None


        mocker.patch("wikipedia_news_downloader.MarkItDown")  # Won't be used

        downloader_instance._worker(mock_queue)

        downloader_instance.logger.error.assert_any_call(f"Unknown mode in queue item: unknown_mode. Item: {('unknown_mode', month_dt, 0)}. Skipping.")
        mock_queue.task_done.assert_called_once()

    def test_source_uri_not_set_due_to_missing_local_html_input_dir_in_offline_mode(  # E501 too long
        self,
        downloader_instance: WikiNewsDownloader,
        mock_queue: MagicMock,
        mocker: Any,
    ) -> None:
        month_dt = datetime(2024, 10, 1)
        mock_queue.get.side_effect = [("offline", month_dt, 0), queue.Empty]
        downloader_instance.local_html_input_dir = None # Ensure it's None for this test


        mocker.patch("wikipedia_news_downloader.MarkItDown")

        downloader_instance._worker(mock_queue)

        downloader_instance.logger.error.assert_any_call("Cannot process offline mode: local_html_input_dir not provided to worker.")
        mock_queue.task_done.assert_called_once()

    def test_worker_handles_empty_markdown_after_conversion(  # E501 too long
        self,
        downloader_instance: WikiNewsDownloader,
        mock_queue: MagicMock,
        mock_markitdown_converter: MagicMock,
        mocker: Any,
    ) -> None:
        month_dt = datetime(2024, 11, 1)
        mock_queue.get.side_effect = [("online", month_dt, 0), queue.Empty]
        downloader_instance.local_html_input_dir = None

        mock_markitdown_converter.convert.return_value = MagicMock(text_content="  ")
        mocker.patch("wikipedia_news_downloader.MarkItDown", return_value=mock_markitdown_converter)
        mock_split_clean = mocker.patch.object(WikiNewsDownloader, "_split_and_clean_monthly_markdown")

        downloader_instance._worker(mock_queue)

        expected_url = f"{downloader_instance.base_wikipedia_url}{month_dt.strftime('%B_%Y')}"
        mock_markitdown_converter.convert.assert_called_once_with(expected_url)
        downloader_instance.logger.warning.assert_any_call(
            f"No content extracted for {month_dt.strftime('%B_%Y')} (mode: online). Skipping further processing.",
        )
        mock_split_clean.assert_not_called()
        mock_queue.task_done.assert_called_once()

    def test_worker_handles_no_daily_events_after_split(  # E501 too long
        self,
        downloader_instance: WikiNewsDownloader,
        mock_queue: MagicMock,
        mock_markitdown_converter: MagicMock,
        mocker: Any,
    ) -> None:
        month_dt = datetime(2024, 12, 1)
        mock_queue.get.side_effect = [("online", month_dt, 0), queue.Empty]
        downloader_instance.local_html_input_dir = None

        mock_markitdown_converter.convert.return_value = MagicMock(text_content="Valid markdown but no daily delimiters")
        mocker.patch("wikipedia_news_downloader.MarkItDown", return_value=mock_markitdown_converter)
        mocker.patch.object(WikiNewsDownloader, "_split_and_clean_monthly_markdown", return_value=[])  # No events
        mock_generate_jekyll = mocker.patch.object(WikiNewsDownloader, "_generate_jekyll_content")

        downloader_instance._worker(mock_queue)

        downloader_instance.logger.warning.assert_any_call(
            f"No daily events found or extracted for {month_dt.strftime('%B_%Y')} (month_dt: {month_dt.strftime('%Y-%B')}).",
        )
        mock_generate_jekyll.assert_not_called()
        mock_queue.task_done.assert_called_once()

    def test_worker_offline_mode_conversion_runtime_error(
        self,
        downloader_instance: WikiNewsDownloader,
        mock_queue: MagicMock,
        mock_markitdown_converter: MagicMock,
        temp_html_input_dir: str,
        mocker: Any,
    ) -> None:
        month_dt = datetime(2024, 7, 1)
        downloader_instance.local_html_input_dir = temp_html_input_dir
        file_name = "july_2024.html"
        html_file_path = Path(temp_html_input_dir) / file_name
        with html_file_path.open("w") as f:
            f.write("<html><body>Dummy HTML for error test</body></html>")

        mock_queue.get.side_effect = [("offline", month_dt, 0), queue.Empty]

        simulated_error = RuntimeError("Simulated conversion error")
        mock_markitdown_converter.convert.side_effect = simulated_error
        mocker.patch("wikipedia_news_downloader.MarkItDown", return_value=mock_markitdown_converter)

        downloader_instance._worker(mock_queue)

        mock_markitdown_converter.convert.assert_called_once_with(html_file_path)
        downloader_instance.logger.exception.assert_any_call(
            f"Error during content conversion or processing for {html_file_path} (local file for July_2024, mode: offline, attempt N/A)",
        )
        mock_queue.put.assert_not_called()
        mock_queue.task_done.assert_called_once()

    def test_worker_online_mode_conversion_runtime_error_retries(
        self,
        downloader_instance: WikiNewsDownloader,
        mock_queue: MagicMock,
        mock_markitdown_converter: MagicMock,
        mocker: Any,
    ) -> None:
        month_dt = datetime(2024, 8, 1)
        downloader_instance.local_html_input_dir = None
        initial_retries = 0
        mock_queue.get.side_effect = [("online", month_dt, initial_retries), queue.Empty]

        simulated_error = RuntimeError("Simulated conversion error")
        mock_markitdown_converter.convert.side_effect = simulated_error
        mocker.patch("wikipedia_news_downloader.MarkItDown", return_value=mock_markitdown_converter)
        mock_time_sleep = mocker.patch("time.sleep")

        downloader_instance._worker(mock_queue)

        expected_url = f"{downloader_instance.base_wikipedia_url}{month_dt.strftime('%B_%Y')}"
        mock_markitdown_converter.convert.assert_called_once_with(expected_url)

        downloader_instance.logger.exception.assert_any_call(
            f"Error during content conversion or processing for {expected_url} "
            f"(online source for August_2024, mode: online, attempt {initial_retries})",
        )

        mock_time_sleep.assert_called_once()
        mock_queue.put.assert_called_once_with(("online", month_dt, initial_retries + 1))
        mock_queue.task_done.assert_called_once()

    def test_worker_online_mode_split_clean_exception_retries(
        self,
        downloader_instance: WikiNewsDownloader,
        mock_queue: MagicMock,
        mock_markitdown_converter: MagicMock,
        mocker: Any,
    ) -> None:
        month_dt = datetime(2024, 9, 1)
        downloader_instance.local_html_input_dir = None
        initial_retries = 0
        mock_queue.get.side_effect = [("online", month_dt, initial_retries), queue.Empty]

        mocker.patch("wikipedia_news_downloader.MarkItDown", return_value=mock_markitdown_converter)
        mock_markitdown_converter.convert.return_value = MagicMock(text_content="Some dummy markdown")

        simulated_error_msg = "Simulated splitting error"
        mock_split_clean = mocker.patch.object(WikiNewsDownloader, "_split_and_clean_monthly_markdown", side_effect=Exception(simulated_error_msg))
        mock_time_sleep = mocker.patch("time.sleep")

        downloader_instance._worker(mock_queue)

        expected_url = f"{downloader_instance.base_wikipedia_url}{month_dt.strftime('%B_%Y')}"
        mock_markitdown_converter.convert.assert_called_once_with(expected_url)
        # _split_and_clean_monthly_markdown no longer takes logger as direct arg
        mock_split_clean.assert_called_once_with("Some dummy markdown", month_dt)

        downloader_instance.logger.exception.assert_any_call(
            f"Error during content conversion or processing for {expected_url} "
            f"(online source for September_2024, mode: online, attempt {initial_retries})",
        )

        mock_time_sleep.assert_called_once()
        mock_queue.put.assert_called_once_with(("online", month_dt, initial_retries + 1))
        mock_queue.task_done.assert_called_once()

    def test_worker_online_mode_jekyll_generation_exception_retries(
        self,
        downloader_instance: WikiNewsDownloader,
        mock_queue: MagicMock,
        mock_markitdown_converter: MagicMock,
        mocker: Any,
    ) -> None:
        month_dt = datetime(2024, 10, 1)
        downloader_instance.local_html_input_dir = None
        initial_retries = 0
        mock_queue.get.side_effect = [("online", month_dt, initial_retries), queue.Empty]

        mocker.patch("wikipedia_news_downloader.MarkItDown", return_value=mock_markitdown_converter)
        mock_markitdown_converter.convert.return_value = MagicMock(text_content="Some dummy markdown")

        daily_event_date = datetime(2024, 10, 1)
        daily_event_content = "Cleaned daily content for Jekyll test"
        mock_split_clean = mocker.patch.object(WikiNewsDownloader, "_split_and_clean_monthly_markdown", return_value=[(daily_event_date, daily_event_content)])

        simulated_error_msg = "Simulated Jekyll generation error"
        mock_generate_jekyll = mocker.patch.object(WikiNewsDownloader, "_generate_jekyll_content", side_effect=Exception(simulated_error_msg))
        mock_time_sleep = mocker.patch("time.sleep")

        downloader_instance._worker(mock_queue)

        expected_url = f"{downloader_instance.base_wikipedia_url}{month_dt.strftime('%B_%Y')}"
        mock_markitdown_converter.convert.assert_called_once_with(expected_url)
        mock_split_clean.assert_called_once_with("Some dummy markdown", month_dt)
        # _generate_jekyll_content no longer takes logger as direct arg
        mock_generate_jekyll.assert_called_once_with(daily_event_date, daily_event_content)

        downloader_instance.logger.exception.assert_any_call(
            f"Error during content conversion or processing for {expected_url} "
            f"(online source for October_2024, mode: online, attempt {initial_retries})",
        )

        mock_time_sleep.assert_called_once()
        mock_queue.put.assert_called_once_with(("online", month_dt, initial_retries + 1))
        mock_queue.task_done.assert_called_once()

    # --- End Tests for worker function ---

    def test_published_false_empty_string_body(self, common_test_date: datetime, downloader_instance: WikiNewsDownloader) -> None: # Added downloader_instance
        markdown_body = ""
        full_content = downloader_instance._generate_jekyll_content(common_test_date, markdown_body) # Use instance
        assert "published: false" in full_content

        # Body should be empty
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


class TestMainFunctionLogging:
    # Patch the global `main`'s dependencies: parse_arguments, setup_logging, and WikiNewsDownloader itself
    @patch("wikipedia_news_downloader.parse_arguments")
    @patch("wikipedia_news_downloader.setup_logging")
    @patch("wikipedia_news_downloader.WikiNewsDownloader") # To check instantiation and run call
    @patch("wikipedia_news_downloader.Path.is_dir") # To control local_html_dir validation
    @patch("wikipedia_news_downloader.Path.glob") # To control what files are "found"
    def test_main_with_provided_logger_and_local_files(
        self,
        mock_path_glob: MagicMock,
        mock_path_is_dir: MagicMock,
        mock_downloader_class: MagicMock,
        mock_setup_logging: MagicMock,
        mock_parse_args: MagicMock,
        # Removed mock_thread and mock_mkdir as they are deeper implementation details
        # of WikiNewsDownloader.run() or __init__ now.
    ) -> None:
        mock_custom_logger = MagicMock(spec=logging.Logger)
        mock_setup_logging.return_value = mock_custom_logger # setup_logging will return our custom logger

        # Mock args returned by parse_arguments
        mock_args = MagicMock()
        mock_args.output_dir = "dummy_output"
        mock_args.verbose = True # Let's say verbose is true
        mock_args.workers = 2
        mock_args.local_html_dir = Path("fake/local_dir")
        mock_parse_args.return_value = mock_args

        # Mock Path checks for local_html_dir
        mock_path_is_dir.return_value = True # Simulate local_html_dir is a valid directory
        dummy_file_path = MagicMock(spec=Path)
        dummy_file_path.name = "file1.html"
        mock_path_glob.return_value = [dummy_file_path] # Simulate finding one HTML file

        # Mock the WikiNewsDownloader instance and its run method
        mock_downloader_instance = MagicMock(spec=WikiNewsDownloader)
        mock_downloader_class.return_value = mock_downloader_instance

        # Call the global main function (which is now parameterless)
        from wikipedia_news_downloader import main as global_main
        global_main()

        # Assert setup_logging was called correctly by main
        mock_setup_logging.assert_called_once_with(mock_args.verbose)

        # Assert WikiNewsDownloader was instantiated correctly by main
        mock_downloader_class.assert_called_once_with(
            output_dir=mock_args.output_dir,
            verbose=mock_args.verbose,
            num_workers=mock_args.workers,
            local_html_input_dir=str(mock_args.local_html_dir),
            logger=mock_custom_logger, # This is key: main should pass the logger from setup_logging
            base_url=mock_downloader_class.BASE_WIKIPEDIA_URL # Use the mocked class's attribute
        )

        # Assert the run method was called on the instance with the correct list of files
        mock_downloader_instance.run.assert_called_once_with(local_html_files_list=[dummy_file_path])

        # We no longer check mock_custom_logger.info directly here for the "Starting processing..." message
        # because that log message is now emitted from within WikiNewsDownloader.run().
        # If we wanted to test that, we'd check mock_custom_logger.info AFTER downloader.run() is called,
        # but the primary goal here is to check that `main` passes the correct logger.

    @patch("wikipedia_news_downloader.parse_arguments")
    @patch("wikipedia_news_downloader.setup_logging")
    @patch("wikipedia_news_downloader.WikiNewsDownloader")
    @patch("wikipedia_news_downloader.Path.is_dir")
    @patch("wikipedia_news_downloader.Path.glob")
    def test_main_with_default_logger_and_no_local_files(
        self,
        mock_path_glob: MagicMock,
        mock_path_is_dir: MagicMock,
        mock_downloader_class: MagicMock,
        mock_setup_logging: MagicMock,
        mock_parse_args: MagicMock,
    ) -> None:
        mock_default_logger = MagicMock(spec=logging.Logger)
        mock_setup_logging.return_value = mock_default_logger

        mock_args = MagicMock()
        mock_args.output_dir = "default_output"
        mock_args.verbose = False
        mock_args.workers = None # Test default worker behavior
        mock_args.local_html_dir = None # No local HTML directory provided
        mock_parse_args.return_value = mock_args

        mock_downloader_instance = MagicMock(spec=WikiNewsDownloader)
        mock_downloader_class.return_value = mock_downloader_instance

        from wikipedia_news_downloader import main as global_main
        global_main()

        mock_setup_logging.assert_called_once_with(mock_args.verbose)

        mock_downloader_class.assert_called_once_with(
            output_dir=mock_args.output_dir,
            verbose=mock_args.verbose,
            num_workers=mock_args.workers,
            local_html_input_dir=None, # Because args.local_html_dir is None
            logger=mock_default_logger, # Logger from setup_logging
            base_url=mock_downloader_class.BASE_WIKIPEDIA_URL # Use the mocked class's attribute
        )
        # html_files_to_pass should be None when args.local_html_dir is None
        mock_downloader_instance.run.assert_called_once_with(local_html_files_list=None)
