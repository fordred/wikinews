import logging
import queue  # For queue.Queue and queue.Empty
from datetime import datetime
from pathlib import Path  # For Path object
from typing import Any  # For fixture type hints
from unittest.mock import MagicMock, patch  # For mocking MarkItDown conversion result and patching

import pytest
import requests  # For requests.exceptions.RequestException

from wikipedia_news_downloader import (
    BASE_WIKIPEDIA_URL,  # Import for use in tests
    MIN_MARKDOWN_LENGTH_PUBLISH,
    clean_daily_markdown_content,
    generate_jekyll_content,
    main,  # Import main for testing
    parse_jekyll_post,
    save_news,
    split_and_clean_monthly_markdown,
    worker,
)

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


# --- End Fixtures ---


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

    def test_split_and_clean_malformed_date(self, mock_logger: MagicMock) -> None:
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
        daily_events = split_and_clean_monthly_markdown(markdown_with_malformed_date, month_dt, mock_logger)

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
        for call in mock_logger.exception.call_args_list:
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


# --- Tests for worker function ---


class TestWorkerFunction:
    def test_offline_mode_valid_html_file(  # PLR0913 potentially, but fixtures are common in pytest
        self,
        mock_logger: MagicMock,
        mock_queue: MagicMock,
        mock_markitdown_converter: MagicMock,
        temp_output_dir: str,
        temp_html_input_dir: str,
        mocker: Any,
    ) -> None:
        month_dt = datetime(2024, 1, 1)
        file_name = "january_2024.html"
        html_file_path = Path(temp_html_input_dir) / file_name
        with html_file_path.open("w") as f:  # PTH123
            f.write("<html><body>Mock HTML</body></html>")

        mock_queue.get.side_effect = [("offline", month_dt), queue.Empty] # Retries removed

        mocker.patch("wikipedia_news_downloader.MarkItDown", return_value=mock_markitdown_converter)
        mocker.patch(
            "wikipedia_news_downloader.split_and_clean_monthly_markdown",
            return_value=[(datetime(2024, 1, 1), "Cleaned daily content")],
        )
        mocker.patch("wikipedia_news_downloader.generate_jekyll_content", return_value="Jekyll content published: true")
        mock_save_news = mocker.patch("wikipedia_news_downloader.save_news")

        # Pass mock_markitdown_converter to worker (now 4th arg)
        worker(mock_queue, temp_output_dir, mock_logger, mock_markitdown_converter, temp_html_input_dir)

        mock_markitdown_converter.convert.assert_called_once_with(html_file_path)
        mock_save_news.assert_called_once()
        mock_queue.task_done.assert_called_once()
        mock_logger.info.assert_any_call(f"Processing in offline mode for {month_dt.strftime('%Y-%B')}")

    def test_offline_mode_missing_html_file(
        self,
        mock_logger: MagicMock,
        mock_queue: MagicMock,
        mock_markitdown_converter: MagicMock,
        temp_output_dir: str,
        temp_html_input_dir: str,
        mocker: Any,
    ) -> None:
        month_dt = datetime(2024, 2, 1)  # February
        mock_queue.get.side_effect = [("offline", month_dt), queue.Empty] # Retries removed

        mocker.patch("wikipedia_news_downloader.MarkItDown", return_value=mock_markitdown_converter)
        # Path.exists will default to False for a non-existent file if not mocked, which is what we want to test

        # Pass mock_markitdown_converter to worker (now 4th arg)
        worker(mock_queue, temp_output_dir, mock_logger, mock_markitdown_converter, temp_html_input_dir)

        expected_file_path = Path(temp_html_input_dir) / "february_2024.html"
        mock_logger.error.assert_any_call(f"Offline mode: Source HTML file not found at {expected_file_path}. Skipping.")
        mock_markitdown_converter.convert.assert_not_called()
        mock_queue.task_done.assert_called_once()

    def test_online_mode_successful_fetch(
        self,
        mock_logger: MagicMock,
        mock_queue: MagicMock,
        mock_markitdown_converter: MagicMock,
        temp_output_dir: str,
        mocker: Any,
    ) -> None:
        month_dt = datetime(2024, 3, 1)
        mock_queue.get.side_effect = [("online", month_dt), queue.Empty] # Retries removed

        mocker.patch("wikipedia_news_downloader.MarkItDown", return_value=mock_markitdown_converter)
        mocker.patch(
            "wikipedia_news_downloader.split_and_clean_monthly_markdown",
            return_value=[(datetime(2024, 3, 1), "Cleaned daily content")],
        )
        mocker.patch("wikipedia_news_downloader.generate_jekyll_content", return_value="Jekyll content published: true")
        mock_save_news = mocker.patch("wikipedia_news_downloader.save_news")

        # Pass mock_markitdown_converter to worker (now 4th arg, local_html_input_dir is None)
        worker(mock_queue, temp_output_dir, mock_logger, mock_markitdown_converter, None)

        expected_url = f"{BASE_WIKIPEDIA_URL}{month_dt.strftime('%B_%Y')}"
        mock_markitdown_converter.convert.assert_called_once_with(expected_url)
        mock_save_news.assert_called_once()
        mock_queue.task_done.assert_called_once()
        mock_logger.info.assert_any_call(f"Processing in online mode for {month_dt.strftime('%Y-%B')}")

    def test_online_mode_request_exception_404_no_retry(
        self,
        mock_logger: MagicMock,
        mock_queue: MagicMock,
        mock_markitdown_converter: MagicMock,
        temp_output_dir: str,
        mocker: Any,
    ) -> None:
        month_dt = datetime(2024, 4, 1)
        mock_queue.get.side_effect = [("online", month_dt), queue.Empty] # Retries removed

        # Simulate a 404 error
        response_mock = MagicMock()
        response_mock.status_code = 404
        error = requests.exceptions.RequestException("Not Found", response=response_mock)
        mock_markitdown_converter.convert.side_effect = error

        mocker.patch("wikipedia_news_downloader.MarkItDown", return_value=mock_markitdown_converter)
        mocker.patch("time.sleep")  # Mock time.sleep to avoid delays

        # Pass mock_markitdown_converter to worker (now 4th arg, local_html_input_dir is None)
        worker(mock_queue, temp_output_dir, mock_logger, mock_markitdown_converter, None)

        expected_url = f"{BASE_WIKIPEDIA_URL}{month_dt.strftime('%B_%Y')}"
        mock_markitdown_converter.convert.assert_called_once_with(expected_url)
        mock_logger.warning.assert_any_call(f"HTTP 404 Not Found for {expected_url} (online source for April_2024). Skipping.")
        mock_queue.put.assert_not_called()  # Should not re-queue for 404
        mock_queue.task_done.assert_called_once()

    def test_online_mode_request_exception_429_retries(
        self,
        mock_logger: MagicMock,
        mock_queue: MagicMock,
        mock_markitdown_converter: MagicMock,
        temp_output_dir: str,
        mocker: Any,
    ) -> None:
        month_dt = datetime(2024, 5, 1)
        # Simulate getting item, then queue becomes empty
        # Retries are now handled by the session, so the mock queue only needs to provide the item once.
        # The test will verify that MarkItDown.convert is called multiple times due to session retries.
        mock_queue.get.side_effect = [("online", month_dt), queue.Empty] # Retries removed from queue item

        response_mock = MagicMock()
        response_mock.status_code = 429
        error = requests.exceptions.RequestException("Too Many Requests", response=response_mock)

        # First call raises 429, second call (retry) succeeds
        mock_markitdown_converter.convert.side_effect = [error, MagicMock(text_content="Successful content after retry")]

        mocker.patch("wikipedia_news_downloader.MarkItDown", return_value=mock_markitdown_converter)
        mocker.patch("time.sleep")
        mocker.patch(
            "wikipedia_news_downloader.split_and_clean_monthly_markdown",
            return_value=[(datetime(2024, 5, 1), "Cleaned daily content")],
        )
        mocker.patch("wikipedia_news_downloader.generate_jekyll_content", return_value="Jekyll content published: true")
        mocker.patch("wikipedia_news_downloader.save_news")

        # Pass mock_markitdown_converter to worker (now 4th arg, local_html_input_dir is None)
        worker(mock_queue, temp_output_dir, mock_logger, mock_markitdown_converter, None)

        expected_url = f"{BASE_WIKIPEDIA_URL}{month_dt.strftime('%B_%Y')}"
        # Since MarkItDown.convert is mocked, session retry is bypassed. Convert is called once.
        mock_markitdown_converter.convert.assert_called_once_with(expected_url)

        mock_logger.warning.assert_any_call(
            f"HTTP 429 Too Many Requests for {expected_url} (online source for May_2024). Relying on session retry.",
        )
        # Ensure no successful processing happened
        mocker.patch("wikipedia_news_downloader.split_and_clean_monthly_markdown").assert_not_called()
        mocker.patch("wikipedia_news_downloader.generate_jekyll_content").assert_not_called()
        mocker.patch("wikipedia_news_downloader.save_news").assert_not_called()

        mock_queue.put.assert_not_called()  # No manual re-queue
        mock_queue.task_done.assert_called_once() # Called once for the single item from queue

    def test_online_mode_generic_request_exception_retries_then_max_out(  # E501 too long
        self,
        mock_logger: MagicMock,
        mock_queue: MagicMock,
        mock_markitdown_converter: MagicMock,
        temp_output_dir: str,
        mocker: Any,
    ) -> None:
        month_dt = datetime(2024, 6, 1)
        # Item is fetched once from the queue. Retries are handled by the session.
        mock_queue.get.side_effect = [("online", month_dt), queue.Empty] # Retries removed

        error = requests.exceptions.RequestException("Some generic network error")
        mock_markitdown_converter.convert.side_effect = error  # Always fails

        mocker.patch("wikipedia_news_downloader.MarkItDown", return_value=mock_markitdown_converter)
        # time.sleep is now handled by the Retry mechanism in the session, so direct patching in worker is not primary.
        # However, if Retry itself uses time.sleep, this could still be relevant for speeding up tests.
        # For now, assume Retry's internal delays are acceptable or will be handled if tests become too slow.
        mocker.patch("time.sleep")

        # Pass mock_markitdown_converter to worker (now 4th arg, local_html_input_dir is None)
        worker(mock_queue, temp_output_dir, mock_logger, mock_markitdown_converter, None)

        expected_url = f"{BASE_WIKIPEDIA_URL}{month_dt.strftime('%B_%Y')}"
        # Since MarkItDown.convert is mocked, session retry is bypassed. Convert is called once.
        mock_markitdown_converter.convert.assert_called_once_with(expected_url)

        # Check that it logged the warning about the error and relying on session retry.
        mock_logger.warning.assert_any_call(
            f"Request error fetching {expected_url} (online source for June_2024): {error}. Relying on session retry.",
        )

        # Ensure "Exceeded max retries" log from worker is NOT present (session handles actual retries)
        for call in mock_logger.error.call_args_list:
            assert "Exceeded max retries" not in call.args[0]

        # Ensure no successful processing happened
        mocker.patch("wikipedia_news_downloader.split_and_clean_monthly_markdown").assert_not_called()
        mocker.patch("wikipedia_news_downloader.generate_jekyll_content").assert_not_called()
        mocker.patch("wikipedia_news_downloader.save_news").assert_not_called()

        mock_queue.put.assert_not_called() # No manual re-queue by the worker
        mock_queue.task_done.assert_called_once() # Task is done once for the item from the queue

    def test_offline_mode_generic_exception_on_convert(  # E501 too long
        self,
        mock_logger: MagicMock,
        mock_queue: MagicMock,
        mock_markitdown_converter: MagicMock,
        temp_output_dir: str,
        temp_html_input_dir: str,
        mocker: Any,
    ) -> None:
        month_dt = datetime(2024, 7, 1)
        file_name = "july_2024.html"
        html_file_path = Path(temp_html_input_dir) / file_name
        with html_file_path.open("w") as f:  # PTH123
            f.write("<html><body>Mock HTML</body></html>")

        mock_queue.get.side_effect = [("offline", month_dt), queue.Empty] # Retries removed

        error = Exception("Something went wrong during conversion")
        mock_markitdown_converter.convert.side_effect = error

        mocker.patch("wikipedia_news_downloader.MarkItDown", return_value=mock_markitdown_converter)

        # Pass mock_markitdown_converter to worker (now 4th arg)
        worker(mock_queue, temp_output_dir, mock_logger, mock_markitdown_converter, temp_html_input_dir)

        mock_markitdown_converter.convert.assert_called_once_with(html_file_path)
        mock_logger.exception.assert_any_call(
            f"Error during content conversion or processing for {html_file_path} (local file for July_2024, mode: offline)",
        )
        mock_queue.put.assert_not_called()  # No retry for offline conversion error
        mock_queue.task_done.assert_called_once()

    def test_online_mode_generic_exception_on_convert_retries(  # E501 too long
        self,
        mock_logger: MagicMock,
        mock_queue: MagicMock,
        mock_markitdown_converter: MagicMock,
        temp_output_dir: str,
        mocker: Any,
    ) -> None:
        month_dt = datetime(2024, 8, 1)
        mock_queue.get.side_effect = [("online", month_dt), queue.Empty] # Item fetched once

        error = Exception("Something went wrong during conversion")
        mock_markitdown_converter.convert.side_effect = error # Always raises this error

        mocker.patch("wikipedia_news_downloader.MarkItDown", return_value=mock_markitdown_converter)
        # time.sleep is not called by worker for generic exceptions
        mock_sleep = mocker.patch("time.sleep")
        mock_split_clean = mocker.patch("wikipedia_news_downloader.split_and_clean_monthly_markdown")
        mock_generate_jekyll = mocker.patch("wikipedia_news_downloader.generate_jekyll_content")
        mock_save_news = mocker.patch("wikipedia_news_downloader.save_news")

        # Pass mock_markitdown_converter as 4th arg, local_html_input_dir is 5th (None)
        worker(mock_queue, temp_output_dir, mock_logger, mock_markitdown_converter, None)

        expected_url = f"{BASE_WIKIPEDIA_URL}{month_dt.strftime('%B_%Y')}"
        mock_markitdown_converter.convert.assert_called_once_with(expected_url) # Called once

        # Check that the exception was logged
        logged_exception_message = False
        for call in mock_logger.exception.call_args_list:
            if f"Error during content conversion or processing for {expected_url}" in call.args[0] and \
               "(online source for August_2024, mode: online)" in call.args[0]: # Retry attempt info removed
                logged_exception_message = True
                break
        assert logged_exception_message, "Generic exception was not logged correctly."

        mock_queue.put.assert_not_called() # No re-queue for generic exception
        mock_sleep.assert_not_called() # Worker does not sleep for this
        mock_split_clean.assert_not_called()
        mock_generate_jekyll.assert_not_called()
        mock_save_news.assert_not_called()
        mock_queue.task_done.assert_called_once() # Task done once

    def test_unknown_mode_in_queue(
        self,
        mock_logger: MagicMock,
        mock_queue: MagicMock,
        mock_markitdown_converter: MagicMock,  # Added fixture injection
        temp_output_dir: str,
        mocker: Any,
    ) -> None:
        month_dt = datetime(2024, 9, 1)
        mock_queue.get.side_effect = [("unknown_mode", month_dt), queue.Empty] # Retries removed

        mocker.patch("wikipedia_news_downloader.MarkItDown")  # Won't be used
        # For this test, worker doesn't get far enough to use md_converter.
        # Pass the injected mock_markitdown_converter as the 4th arg, and None as the 5th.
        worker(mock_queue, temp_output_dir, mock_logger, mock_markitdown_converter, None)

        mock_logger.error.assert_any_call(f"Unknown mode in queue item: unknown_mode. Item: {('unknown_mode', month_dt)}. Skipping.")
        mock_queue.task_done.assert_called_once()

    def test_source_uri_not_set_due_to_missing_local_html_input_dir_in_offline_mode(  # E501 too long
        self,
        mock_logger: MagicMock,
        mock_queue: MagicMock,
        mock_markitdown_converter: MagicMock,  # Added fixture injection
        temp_output_dir: str,
        mocker: Any,
    ) -> None:
        month_dt = datetime(2024, 10, 1)
        mock_queue.get.side_effect = [("offline", month_dt), queue.Empty] # Retries removed

        mocker.patch("wikipedia_news_downloader.MarkItDown")  # Won't be used

        # Crucially, local_html_input_dir is None, which should trigger the "cannot process offline mode" error path.
        # This happens before source_uri would be checked.
        # md_converter (4th arg) is not used if local_html_input_dir (5th arg) is None for offline mode.
        # Pass the injected mock_markitdown_converter as the 4th arg.
        worker(mock_queue, temp_output_dir, mock_logger, mock_markitdown_converter, None)

        mock_logger.error.assert_any_call("Cannot process offline mode: local_html_input_dir not provided to worker.")
        # The more specific "Source URI not set" error shouldn't be hit in this case due to the early exit.
        # The primary check is that the specific error about local_html_input_dir is logged.
        mock_queue.task_done.assert_called_once()

    def test_worker_handles_empty_markdown_after_conversion(  # E501 too long
        self,
        mock_logger: MagicMock,
        mock_queue: MagicMock,
        mock_markitdown_converter: MagicMock,
        temp_output_dir: str,
        mocker: Any,
    ) -> None:
        month_dt = datetime(2024, 11, 1)
        mock_queue.get.side_effect = [("online", month_dt), queue.Empty] # Retries removed

        mock_markitdown_converter.convert.return_value = MagicMock(text_content="  ")  # Empty/whitespace content
        mocker.patch("wikipedia_news_downloader.MarkItDown", return_value=mock_markitdown_converter)
        mock_split_clean = mocker.patch("wikipedia_news_downloader.split_and_clean_monthly_markdown")

        # Pass mock_markitdown_converter to worker (now 4th arg, local_html_input_dir is None)
        worker(mock_queue, temp_output_dir, mock_logger, mock_markitdown_converter, None)

        expected_url = f"{BASE_WIKIPEDIA_URL}{month_dt.strftime('%B_%Y')}"
        mock_markitdown_converter.convert.assert_called_once_with(expected_url)
        mock_logger.warning.assert_any_call(
            f"No content extracted for {month_dt.strftime('%B_%Y')} (mode: online). Skipping further processing.",
        )
        mock_split_clean.assert_not_called()  # Should not proceed to split/clean
        mock_queue.task_done.assert_called_once()

    def test_worker_handles_no_daily_events_after_split(  # E501 too long
        self,
        mock_logger: MagicMock,
        mock_queue: MagicMock,
        mock_markitdown_converter: MagicMock,
        temp_output_dir: str,
        mocker: Any,
    ) -> None:
        month_dt = datetime(2024, 12, 1)
        mock_queue.get.side_effect = [("online", month_dt), queue.Empty] # Retries removed

        mock_markitdown_converter.convert.return_value = MagicMock(text_content="Valid markdown but no daily delimiters")
        mocker.patch("wikipedia_news_downloader.MarkItDown", return_value=mock_markitdown_converter)
        mocker.patch("wikipedia_news_downloader.split_and_clean_monthly_markdown", return_value=[])  # No events
        mock_generate_jekyll = mocker.patch("wikipedia_news_downloader.generate_jekyll_content")

        # Pass mock_markitdown_converter to worker (now 4th arg, local_html_input_dir is None)
        worker(mock_queue, temp_output_dir, mock_logger, mock_markitdown_converter, None)

        mock_logger.warning.assert_any_call(
            f"No daily events found or extracted for {month_dt.strftime('%B_%Y')} (month_dt: {month_dt.strftime('%Y-%B')}).",
        )
        mock_generate_jekyll.assert_not_called()
        mock_queue.task_done.assert_called_once()

    def test_worker_offline_mode_conversion_runtime_error(
        self,
        mock_logger: MagicMock,
        mock_queue: MagicMock,
        mock_markitdown_converter: MagicMock,
        temp_output_dir: str,
        temp_html_input_dir: str,
        mocker: Any,
    ) -> None:
        month_dt = datetime(2024, 7, 1)  # Example date: July 2024
        file_name = "july_2024.html"
        html_file_path = Path(temp_html_input_dir) / file_name
        with html_file_path.open("w") as f:
            f.write("<html><body>Dummy HTML for error test</body></html>")

        mock_queue.get.side_effect = [("offline", month_dt), queue.Empty] # Retries removed

        simulated_error = RuntimeError("Simulated conversion error")
        mock_markitdown_converter.convert.side_effect = simulated_error
        mocker.patch("wikipedia_news_downloader.MarkItDown", return_value=mock_markitdown_converter)

        # Pass mock_markitdown_converter to worker (now 4th arg)
        worker(mock_queue, temp_output_dir, mock_logger, mock_markitdown_converter, temp_html_input_dir)

        mock_markitdown_converter.convert.assert_called_once_with(html_file_path)

        # Check logger.exception was called with the correct message parts
        # The F841 for exception_logged is fixed by removing the loop and variable.
        # The assert_any_call below is the primary check for the log message.
        mock_logger.exception.assert_any_call(
            f"Error during content conversion or processing for {html_file_path} (local file for July_2024, mode: offline)",
        )

        mock_queue.put.assert_not_called()  # No retries for offline conversion errors
        mock_queue.task_done.assert_called_once()

    def test_worker_online_mode_conversion_runtime_error_retries(
        self,
        mock_logger: MagicMock,
        mock_queue: MagicMock,
        mock_markitdown_converter: MagicMock,
        temp_output_dir: str,
        mocker: Any,
    ) -> None:
        month_dt = datetime(2024, 8, 1)  # Example date: August 2024
        mock_queue.get.side_effect = [("online", month_dt), queue.Empty] # Item fetched once

        simulated_error = RuntimeError("Simulated conversion error")
        mock_markitdown_converter.convert.side_effect = simulated_error # Always raises
        mocker.patch("wikipedia_news_downloader.MarkItDown", return_value=mock_markitdown_converter)

        # Worker does not retry or sleep for this type of error
        mock_time_sleep = mocker.patch("time.sleep")
        mock_split_clean = mocker.patch("wikipedia_news_downloader.split_and_clean_monthly_markdown")
        mock_generate_jekyll = mocker.patch("wikipedia_news_downloader.generate_jekyll_content")
        mock_save_news = mocker.patch("wikipedia_news_downloader.save_news")

        # Pass mock_markitdown_converter to worker (now 4th arg, local_html_input_dir is None)
        worker(mock_queue, temp_output_dir, mock_logger, mock_markitdown_converter, None)

        expected_url = f"{BASE_WIKIPEDIA_URL}{month_dt.strftime('%B_%Y')}"
        mock_markitdown_converter.convert.assert_called_once_with(expected_url) # Called once

        # Check logger.exception was called
        logged_exception_message = False
        for call in mock_logger.exception.call_args_list:
            if f"Error during content conversion or processing for {expected_url}" in call.args[0] and \
               "(online source for August_2024, mode: online)" in call.args[0]: # Retry attempt info removed
                logged_exception_message = True
                break
        assert logged_exception_message, "RuntimeError during conversion was not logged correctly."

        mock_time_sleep.assert_not_called()
        mock_queue.put.assert_not_called()
        mock_split_clean.assert_not_called()
        mock_generate_jekyll.assert_not_called()
        mock_save_news.assert_not_called()
        mock_queue.task_done.assert_called_once()

    def test_worker_online_mode_split_clean_exception_retries(
        self,
        mock_logger: MagicMock,
        mock_queue: MagicMock,
        mock_markitdown_converter: MagicMock,  # Provided by fixture, but we'll re-patch MarkItDown
        temp_output_dir: str,
        mocker: Any,
    ) -> None:
        month_dt = datetime(2024, 9, 1)  # Example date: September 2024
        mock_queue.get.side_effect = [("online", month_dt), queue.Empty] # Item fetched once

        # Configure MarkItDown().convert to return a successful mock result
        mocker.patch("wikipedia_news_downloader.MarkItDown", return_value=mock_markitdown_converter)
        mock_markitdown_converter.convert.return_value = MagicMock(text_content="Some dummy markdown")

        # Mock split_and_clean_monthly_markdown to raise an exception
        simulated_error_msg = "Simulated splitting error"
        mock_split_clean = mocker.patch(
            "wikipedia_news_downloader.split_and_clean_monthly_markdown",
            side_effect=Exception(simulated_error_msg),
        )

        mock_time_sleep = mocker.patch("time.sleep")
        mock_generate_jekyll = mocker.patch("wikipedia_news_downloader.generate_jekyll_content")
        mock_save_news = mocker.patch("wikipedia_news_downloader.save_news")

        # Pass mock_markitdown_converter as 4th arg, local_html_input_dir is 5th (None)
        worker(mock_queue, temp_output_dir, mock_logger, mock_markitdown_converter, None)

        expected_url = f"{BASE_WIKIPEDIA_URL}{month_dt.strftime('%B_%Y')}"
        mock_markitdown_converter.convert.assert_called_once_with(expected_url)
        mock_split_clean.assert_called_once_with("Some dummy markdown", month_dt, mock_logger)

        # Check logger.exception was called
        logged_exception_message = False
        for call in mock_logger.exception.call_args_list:
            if f"Error during content conversion or processing for {expected_url}" in call.args[0] and \
               "(online source for September_2024, mode: online)" in call.args[0]: # Retry info removed
                logged_exception_message = True
                break
        assert logged_exception_message, "Exception during split/clean was not logged correctly."

        mock_time_sleep.assert_not_called()
        mock_queue.put.assert_not_called()
        mock_generate_jekyll.assert_not_called() # Should not be reached
        mock_save_news.assert_not_called() # Should not be reached
        mock_queue.task_done.assert_called_once()

    def test_worker_calls_save_news_after_successful_processing(
        self,
        mock_logger: MagicMock,
        mock_queue: MagicMock,
        mock_markitdown_converter: MagicMock,
        temp_output_dir: str,
        mocker: Any,
    ) -> None:
        """Tests that the worker calls save_news with correct args after processing."""
        month_dt = datetime(2024, 10, 1)
        mock_queue.get.side_effect = [("online", month_dt), queue.Empty]

        mocker.patch("wikipedia_news_downloader.MarkItDown", return_value=mock_markitdown_converter)
        mock_markitdown_converter.convert.return_value = MagicMock(text_content="Some dummy markdown")

        daily_event_date = datetime(2024, 10, 1)
        daily_event_content = "Cleaned daily content for save_news test"
        mocker.patch(
            "wikipedia_news_downloader.split_and_clean_monthly_markdown",
            return_value=[(daily_event_date, daily_event_content)],
        )

        mock_save_news = mocker.patch("wikipedia_news_downloader.save_news")

        worker(mock_queue, temp_output_dir, mock_logger, mock_markitdown_converter, None)

        mock_save_news.assert_called_once_with(daily_event_date, daily_event_content, temp_output_dir, mock_logger)

    # --- End Tests for worker function ---

    def test_published_false_empty_string_body(self, common_test_date: datetime) -> None:
        markdown_body = ""
        full_content = generate_jekyll_content(common_test_date, markdown_body, logger)
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
    @patch("wikipedia_news_downloader.Path.mkdir")
    @patch("threading.Thread")
    @patch("wikipedia_news_downloader.setup_logging")
    def test_main_with_provided_logger(self, mock_setup_logging: MagicMock, mock_thread: MagicMock, mock_mkdir: MagicMock) -> None:
        _ = mock_mkdir  # Mark as intentionally unused, but keep patch active
        mock_custom_logger = MagicMock(spec=logging.Logger)

        dummy_file_path = MagicMock(spec=Path)
        dummy_file_path.is_file.return_value = True
        dummy_file_path.suffix = ".html"
        dummy_file_path.stem = "january_2025"
        dummy_file_path.name = "january_2025.html"
        mock_parent_dir = MagicMock(spec=Path)
        dummy_file_path.parent = mock_parent_dir

        # Mock queue.Queue().qsize() to return 1 to indicate items to process
        with patch("queue.Queue") as mock_queue_class:
            mock_queue_instance = MagicMock()
            mock_queue_instance.qsize.return_value = 1  # Indicates one item from local_html_files_list
            mock_queue_class.return_value = mock_queue_instance

            main(
                output_dir_str="dummy_output",
                verbose=False,
                num_workers=1,
                local_html_files_list=[dummy_file_path],
                logger=mock_custom_logger,
            )

        mock_setup_logging.assert_not_called()
        # More specific check for the logger call related to starting processing
        # Note: The exact string might need adjustment based on how str(mock_parent_dir) renders.
        # If mock_parent_dir.name is "parent_dir_mock", str(mock_parent_dir) might be "parent_dir_mock".
        # For robustness, we can convert the mock_parent_dir to string for the assertion.
        expected_parent_dir_str = str(mock_parent_dir)
        # Construct the exact expected log message
        expected_log_message = (
            f"Starting processing in mode: local HTML files provided programmatically (input dir: {expected_parent_dir_str}). "
            f"Output directory: dummy_output. "
            f"Input dir for offline (if applicable): {expected_parent_dir_str}. "
            f"Using up to 1 worker thread(s)."
        )
        mock_custom_logger.info.assert_any_call(expected_log_message)

        mock_thread.assert_called_once()
        # Args for Thread: (target=worker, args=(processing_queue, current_output_dir, logger, effective_local_html_input_dir_str))
        # mock_thread.call_args[1]['args'] is the tuple of arguments passed to the worker function.
        thread_args_for_worker = mock_thread.call_args[1]["args"]
        assert len(thread_args_for_worker) >= 4, "Not enough arguments passed to worker thread target"
        assert thread_args_for_worker[2] is mock_custom_logger  # logger is the 3rd arg for worker

    @patch("wikipedia_news_downloader.Path.mkdir")
    @patch("threading.Thread")
    @patch("wikipedia_news_downloader.setup_logging")
    def test_main_with_default_logger(self, mock_setup_logging: MagicMock, mock_thread: MagicMock, mock_mkdir: MagicMock) -> None:
        _ = mock_mkdir  # Mark as intentionally unused, but keep patch active
        mock_default_logger_instance = MagicMock(spec=logging.Logger)
        mock_setup_logging.return_value = mock_default_logger_instance

        dummy_file_path = MagicMock(spec=Path)
        dummy_file_path.is_file.return_value = True
        dummy_file_path.suffix = ".html"
        dummy_file_path.stem = "february_2025"  # Different stem to avoid potential test interactions if any
        dummy_file_path.name = "february_2025.html"
        mock_parent_dir = MagicMock(spec=Path)
        dummy_file_path.parent = mock_parent_dir

        with patch("queue.Queue") as mock_queue_class:
            mock_queue_instance = MagicMock()
            mock_queue_instance.qsize.return_value = 1
            mock_queue_class.return_value = mock_queue_instance

            main(output_dir_str="dummy_output_default", verbose=False, num_workers=1, local_html_files_list=[dummy_file_path], logger=None)

        mock_setup_logging.assert_called_once_with(False)
        expected_parent_dir_str = str(mock_parent_dir)
        # Construct the exact expected log message
        expected_log_message = (
            f"Starting processing in mode: local HTML files provided programmatically (input dir: {expected_parent_dir_str}). "
            f"Output directory: dummy_output_default. "
            f"Input dir for offline (if applicable): {expected_parent_dir_str}. "
            f"Using up to 1 worker thread(s)."
        )
        mock_default_logger_instance.info.assert_any_call(expected_log_message)

        mock_thread.assert_called_once()
        thread_args_for_worker = mock_thread.call_args[1]["args"]
        assert len(thread_args_for_worker) >= 4
        assert thread_args_for_worker[2] is mock_default_logger_instance


class TestSaveNews:
    def test_save_new_post(self, temp_output_dir: str, common_test_date: datetime, mock_logger: MagicMock) -> None:
        """Test that a new post gets a last_modified_at tag."""
        markdown_body = "This is the body of a new post."

        with patch("wikipedia_news_downloader.datetime") as mock_dt:
            mock_dt.now.return_value = datetime(2025, 9, 2, 12, 0, 0)

            save_news(common_test_date, markdown_body, temp_output_dir, mock_logger)

        post_path = Path(temp_output_dir) / (common_test_date.strftime("%Y-%m-%d") + "-index.md")
        assert post_path.exists()

        content = post_path.read_text()
        front_matter, body = parse_jekyll_post(content)

        assert "last_modified_at" in front_matter
        assert front_matter["last_modified_at"] == "2025-09-02 12:00"
        assert body.strip() == markdown_body.strip()

    def test_save_post_with_changed_content(self, temp_output_dir: str, common_test_date: datetime, mock_logger: MagicMock) -> None:
        """Test that a changed post gets an updated last_modified_at tag."""
        post_path = Path(temp_output_dir) / (common_test_date.strftime("%Y-%m-%d") + "-index.md")

        # Create an initial post
        initial_body = "Initial content."
        initial_lmod = "2025-09-01 10:00"
        initial_content = f"---\nlayout: post\ntitle: Test\ndate: {common_test_date.strftime('%Y-%m-%d')}\nlast_modified_at: {initial_lmod}\npublished: true\n---\n\n{initial_body}"
        post_path.write_text(initial_content)

        new_markdown_body = "This is the updated body of the post."

        with patch("wikipedia_news_downloader.datetime") as mock_dt:
            mock_dt.now.return_value = datetime(2025, 9, 2, 13, 0, 0)

            save_news(common_test_date, new_markdown_body, temp_output_dir, mock_logger)

        assert post_path.exists()

        content = post_path.read_text()
        front_matter, body = parse_jekyll_post(content)

        assert "last_modified_at" in front_matter
        assert front_matter["last_modified_at"] == "2025-09-02 13:00"
        assert body.strip() == new_markdown_body.strip()

    def test_save_post_with_unchanged_content_and_existing_tag(
        self, temp_output_dir: str, common_test_date: datetime, mock_logger: MagicMock
    ) -> None:
        """Test that an unchanged post with a tag preserves the tag."""
        post_path = Path(temp_output_dir) / (common_test_date.strftime("%Y-%m-%d") + "-index.md")

        initial_body = "Content that will not change."
        initial_lmod = "2025-09-01 11:00"
        initial_content = f"---\nlayout: post\ntitle: Test\ndate: {common_test_date.strftime('%Y-%m-%d')}\nlast_modified_at: {initial_lmod}\npublished: true\n---\n\n{initial_body}"
        post_path.write_text(initial_content)

        save_news(common_test_date, initial_body, temp_output_dir, mock_logger)

        content = post_path.read_text()
        front_matter, body = parse_jekyll_post(content)

        assert "last_modified_at" in front_matter
        assert front_matter["last_modified_at"] == initial_lmod
        assert body.strip() == initial_body.strip()

    def test_save_post_with_unchanged_content_and_no_tag(
        self, temp_output_dir: str, common_test_date: datetime, mock_logger: MagicMock
    ) -> None:
        """Test that an unchanged post without a tag does not get one."""
        post_path = Path(temp_output_dir) / (common_test_date.strftime("%Y-%m-%d") + "-index.md")

        initial_body = "Content that will not change."
        initial_content = f"---\nlayout: post\ntitle: Test\ndate: {common_test_date.strftime('%Y-%m-%d')}\npublished: true\n---\n\n{initial_body}"
        post_path.write_text(initial_content)

        save_news(common_test_date, initial_body, temp_output_dir, mock_logger)

        content = post_path.read_text()
        front_matter, body = parse_jekyll_post(content)

        assert "last_modified_at" not in front_matter
        assert body.strip() == initial_body.strip()


class TestMainFunctionExecution:
    @patch("wikipedia_news_downloader.sys.exit")
    @patch("wikipedia_news_downloader.MarkItDown")
    @patch("wikipedia_news_downloader.setup_logging")
    def test_main_exits_on_403_error(
        self,
        mock_setup_logging: MagicMock,
        mock_markitdown_class: MagicMock,
        mock_sys_exit: MagicMock,
        tmp_path: Path,
    ) -> None:
        """
        Test that the main function exits with code 1 if a worker encounters a 403 error.
        """
        # Configure the mock MarkItDown converter to raise a 403 error
        mock_converter_instance = MagicMock()
        response_mock = MagicMock()
        response_mock.status_code = 403
        error = requests.exceptions.RequestException("Forbidden", response=response_mock)
        mock_converter_instance.convert.side_effect = error
        mock_markitdown_class.return_value = mock_converter_instance

        # Mock the logger
        mock_logger = MagicMock(spec=logging.Logger)
        mock_setup_logging.return_value = mock_logger

        output_dir = tmp_path / "output"

        # Run the main function in online mode
        # We expect this to encounter the 403 and trigger sys.exit(1)
        main(
            output_dir_str=str(output_dir),
            verbose=False,
            num_workers=1,
            local_html_files_list=None,
            logger=mock_logger,
        )

        # Assert that sys.exit(1) was called
        mock_sys_exit.assert_called_once_with(1)
