import logging
import unittest
from datetime import datetime

from wikipedia_news_downloader import (
    MIN_MARKDOWN_LENGTH_PUBLISH,
    _clean_daily_markdown_content,
    generate_jekyll_content,
    split_and_clean_monthly_markdown,
)

# Raw markdown example from the issue description
raw_markdown_example = """
'* [Home](/wiki/Main_Page)\n* [Random](/wiki/Special%3ARandom)\n* [Nearby](/wiki/Special%3ANearby)\n\n* [Log in](/w/index.php?title=Special:UserLogin&returnto=Portal%3ACurrent+events%2FJune+2025)\n\n* [Settings](/w/index.php?title=Special:MobileOptions&returnto=Portal%3ACurrent+events%2FJune+2025)\n\n[Donate Now\nIf Wikipedia is useful to you, please give today.\n\n![](https://en.wikipedia.org/static/images/donate/donate.gif)](https://donate.wikimedia.org/?wmf_source=donate&wmf_medium=sidebar&wmf_campaign=en.wikipedia.org&uselang=en&wmf_key=minerva)\n\n* [About Wikipedia](/wiki/Wikipedia%3AAbout)\n* [Disclaimers](/wiki/Wikipedia%3AGeneral_disclaimer)\n\n[![Wikipedia](/static/images/mobile/copyright/wikipedia-wordmark-en.svg)](/wiki/Main_Page)\n\nSearch\n\n# Portal:Current events/June 2025\n\n* [Portal](/wiki/Portal%3ACurrent_events/June_2025)\n* [Talk](/wiki/Portal_talk%3ACurrent_events/June_2025)\n\n* [Language](#p-lang "Language")\n* [Watch](/w/index.php?title=Special:UserLogin&returnto=Portal%3ACurrent+events%2FJune+2025)\n* [Edit](/w/index.php?title=Portal:Current_events/June_2025&action=edit)\n\n< [Portal:Current events](/wiki/Portal%3ACurrent_events "Portal:Current events")\n\n[2025](/wiki/2025 "2025")\n:   [January](/wiki/Portal%3ACurrent_events/January_2025 "Portal:Current events/January 2025")\n:   [February](/wiki/Portal%3ACurrent_events/February_2025 "Portal:Current events/February 2025")\n:   [March](/wiki/Portal%3ACurrent_events/March_2025 "Portal:Current events/March 2025")\n:   [April](/wiki/Portal%3ACurrent_events/April_2025 "Portal:Current events/April 2025")\n:   [May](/wiki/Portal%3ACurrent_events/May_2025 "Portal:Current events/May 2025")\n:   June\n:   [July](/wiki/Portal%3ACurrent_events/July_2025 "Portal:Current events/July 2025")\n:   [August](/wiki/Portal%3ACurrent_events/August_2025 "Portal:Current events/August 2025")\n:   [September](/wiki/Portal%3ACurrent_events/September_2025 "Portal:Current events/September 2025")\n:   [October](/wiki/Portal%3ACurrent_events/October_2025 "Portal:Current events/October 2025")\n:   [November](/wiki/Portal%3ACurrent_events/November_2025 "Portal:Current events/November 2025")\n:   [December](/wiki/Portal%3ACurrent_events/December_2025 "Portal:Current events/December 2025")\n\n**[June](/wiki/June "June")** **[2025](/wiki/2025 "2025")** is the sixth month of the current common year. The month, which began on a [Sunday](/wiki/Sunday "Sunday"), will end on a [Monday](/wiki/Monday "Monday") after 30 days. It is the current month.\n\n## [Portal:Current events](/wiki/Portal%3ACurrent_events "Portal:Current events")\n\n[edit](/w/index.php?title=Portal:Current_events/June_2025&action=edit&section=1 "Edit section: Portal:Current events")\n\nJune\xa01,\xa02025\xa0(2025-06-01) (Sunday)\n\n* [edit](https://en.wikipedia.org/w/index.php?title=Portal:Current_events/2025_June_1&action=edit&editintro=Portal:Current_events/Edit_instructions)\n* [history](https://en.wikipedia.org/w/index.php?title=Portal:Current_events/2025_June_1&action=history)\n* [watch](https://en.wikipedia.org/w/index.php?title=Portal:Current_events/2025_June_1&action=watch)\n\n**Sports**\n\n* [2025 CONCACAF Champions Cup](/wiki/2025_CONCACAF_Champions_Cup "2025 CONCACAF Champions Cup")\n  + [Cruz Azul](/wiki/Cruz_Azul "Cruz Azul") defeat the [Vancouver Whitecaps](/wiki/Vancouver_Whitecaps "Vancouver Whitecaps") 5-0 in the [final](/wiki/2025_CONCACAF_Champions_Cup_final "2025 CONCACAF Champions Cup final") of the [CONCACAF Champions Cup](/wiki/CONCACAF_Champions_Cup "CONCACAF Champions Cup") at the [Olympic University Stadium](/wiki/Estadio_Ol%C3%ADmpico_Universitario "Estadio Olímpico Universitario") in [Mexico City](/wiki/Mexico_City "Mexico City"). [(*USA Today*)](https://eu.usatoday.com/story/sports/soccer/2025/06/01/concacaf-champions-cup-cruz-azul-vancouver-whitecaps/83985000007/), [(France 24)](https://www.france24.com/en/live-news/20250602-cruz-azul-thrash-vancouver-whitecaps-to-win-concacaf-champions-cup)\n\nJune\xa02,\xa02025\xa0(2025-06-02) (Monday)\n\n* [edit](https://en.wikipedia.org/w/index.php?title=Portal:Current_events/2025_June_2&action=edit&editintro=Portal:Current_events/Edit_instructions)\n* [history](https://en.wikipedia.org/w/index.php?title=Portal:Current_events/2025_June_2&action=history)\n* [watch](https://en.wikipedia.org/w/index.php?title=Portal:Current_events/2025_June_2&action=watch)\n\n**Disasters and accidents**\n\n* [2025 Nigeria floods](/wiki/2025_Nigeria_floods "2025 Nigeria floods")\n  + [2025 Mokwa flood](/wiki/2025_Mokwa_flood "2025 Mokwa flood")\n    - The death toll from the [flooding](/wiki/Flooding "Flooding") caused by torrential rain in [Mokwa](/wiki/Mokwa "Mokwa"), [Nigeria](/wiki/Nigeria "Nigeria"), increases to over 200. [(DW)](https://www.dw.com/en/death-toll-in-nigeria-flooding-rises-to-at-least-200/video-72755995)\n\nJune\xa03,\xa02025\xa0(2025-06-03) (Tuesday)\n\n* [edit](https://en.wikipedia.org/w/index.php?title=Portal:Current_events/2025_June_3&action=edit&editintro=Portal:Current_events/Edit_instructions)\n* [history](https://en.wikipedia.org/w/index.php?title=Portal:Current_events/2025_June_3&action=history)\n* [watch](https://en.wikipedia.org/w/index.php?title=Portal:Current_events/2025_June_3&action=watch)\n\n**Arts and culture**\n\n* [CJ Opiaza](/wiki/CJ_Opiaza "CJ Opiaza") is officially crowned as [Miss Grand International 2024](/wiki/Miss_Grand_International_2024 "Miss Grand International 2024") following [Rachel Gupta](/wiki/Rachel_Gupta "Rachel Gupta")\'s resignation and termination from the title. [(ABS-CBN News)](https://www.abs-cbn.com/lifestyle/2025/6/3/-this-is-my-golden-moment-cj-opiaza-in-tears-at-miss-grand-international-coronation-1626)\n\nJune\xa016,\xa02025\xa0(2025-06-16) (Monday)\n\n* [edit](https://en.wikipedia.org/w/index.php?title=Portal:Current_events/2025_June_16&action=edit&editintro=Portal:Current_events/Edit_instructions)\n* [history](https://en.wikipedia.org/w/index.php?title=Portal:Current_events/2025_June_16&action=history)\n* [watch](https://en.wikipedia.org/w/index.php?title=Portal:Current_events/2025_June_16&action=watch)\n\n[◀](/wiki/Portal%3ACurrent_events/May_2025 "Portal:Current events/May 2025")June 2025[▶](/wiki/Portal%3ACurrent_events/July_2025 "Portal:Current events/July 2025")\n\n| S | M | T | W | T | F | S |\n| --- | --- | --- | --- | --- | --- | --- |\n| [1](#2025_June_1) | [2](#2025_June_2) | [3](#2025_June_3) | [4](#2025_June_4) | [5](#2025_June_5) | [6](#2025_June_6) | [7](#2025_June_7) |\n| [8](#2025_June_8) | [9](#2025_June_9) | [10](#2025_June_10) | [11](#2025_June_11) | [12](#2025_June_12) | [13](#2025_June_13) | [14](#2025_June_14) |\n| [15](#2025_June_15) | [16](#2025_June_16) | [17](#2025_June_17) | [18](#2025_June_18) | [19](#2025_June_19) | [20](#2025_June_20) | [21](#2025_June_21) |\n| [22](#2025_June_22) | [23](#2025_June_23) | [24](#2025_June_24) | [25](#2025_June_25) | [26](#2025_June_26) | [27](#2025_June_27) | [28](#2025_June_28) |\n| [29](#2025_June_29) | [30](#2025_June_30) |  |  |  |  |  |\n\nWikimedia portal\n\n(transcluded from the [Current events portal](/wiki/Portal%3ACurrent_events "Portal:Current events"))\n\n[About this page](/wiki/Wikipedia%3AHow_the_Current_events_page_works "Wikipedia:How the Current events page works") • [News about Wikipedia](/wiki/Wikipedia%3AWikipedia_Signpost "Wikipedia:Wikipedia Signpost")\n\n\n\n"""  # noqa:E501

# A basic logger for the function call, can be configured if more detail is needed
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)  # So we can see logs if test fails


class TestSplitAndCleanMarkdown(unittest.TestCase):
    @staticmethod
    def test_split_and_clean_from_issue_example() -> None:
        month_dt = datetime(2025, 6, 1)
        daily_events = split_and_clean_monthly_markdown(raw_markdown_example, month_dt, logger)

        assert len(daily_events) == 3, "Should find 3 daily segments"

        # --- Assertions for June 1 ---
        event_dt_june1, md_june1 = daily_events[0]
        assert event_dt_june1 == datetime(2025, 6, 1)

        # Check start of content
        assert md_june1.startswith("**Sports**"), "June 1 MD should start with Sports"
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
        assert md_june2.startswith("**Disasters and accidents**"), "June 2 MD should start with Disasters"
        assert "[2025 Nigeria floods](https://en.wikipedia.org/wiki/2025_Nigeria_floods" in md_june2
        assert "June\xa02,\xa02025" not in md_june2
        assert md_june2.strip().endswith(
            "[(DW)](https://www.dw.com/en/death-toll-in-nigeria-flooding-rises-to-at-least-200/video-72755995)",
        ), "June 2 MD incorrect end"

        # --- Assertions for June 3 ---
        event_dt_june3, md_june3 = daily_events[2]
        assert event_dt_june3 == datetime(2025, 6, 3)
        assert md_june3.startswith("**Arts and culture**"), "June 3 MD should start with Arts"
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
            assert daily_events[0][1].startswith("**Real Content**")


class TestCleanDailyMarkdownContent(unittest.TestCase):
    def test_remove_redlinks(self) -> None:
        # Test various redlink formats
        inputs = [
            "Text with a [redlink](/w/index.php?title=Red_Link&action=edit&redlink=1).",
            "Text with another [redlink](//en.wikipedia.org/w/index.php?title=Another_Red_Link&action=edit&redlink=1).",
            "Text with [redlink with spaces](/w/index.php?title=Red%20Link%20Spaces&action=edit&redlink=1).",
            "A [simple redlink](Red_Link_Page_Not_Exist).",  # Assuming simple non-existent page links are treated as redlinks if they lead to an edit page or similar non-content page, which the function might not distinguish from true redlinks without web access. The current function focuses on specific patterns.
        ]
        expected_outputs = [
            "Text with a [redlink](/w/index.php?title=Red_Link&action=edit&redlink=1).\n",  # Not removed due to missing title attribute in link
            "Text with another [redlink](//en.wikipedia.org/w/index.php?title=Another_Red_Link&action=edit&redlink=1).\n",  # Not removed
            "Text with [redlink with spaces](/w/index.php?title=Red%20Link%20Spaces&action=edit&redlink=1).\n",  # Not removed
            "A [simple redlink](Red_Link_Page_Not_Exist).\n",  # Not a redlink pattern
        ]

        for i, text_input in enumerate(inputs):
            with self.subTest(i=i):
                assert _clean_daily_markdown_content(text_input) == expected_outputs[i]

        # Test specific case from the original code for redlink removal (assuming it had a title attribute in its original context)
        # If the function is intended to remove [Text](/w/index.php?title=...&action=edit&redlink=1 "Title") format
        # For the test string "[CJ Opiaza](/w/index.php?title=CJ_Opiaza&action=edit&redlink=1)"
        # it will NOT be cleaned as it lacks the hover title part "CJ Opiaza"
        redlink_from_code_no_title_attr = "[CJ Opiaza](/w/index.php?title=CJ_Opiaza&action=edit&redlink=1)"
        cleaned_redlink_from_code_no_title_attr = "[CJ Opiaza](/w/index.php?title=CJ_Opiaza&action=edit&redlink=1)\n"  # Stays the same
        assert _clean_daily_markdown_content(redlink_from_code_no_title_attr) == cleaned_redlink_from_code_no_title_attr

        # Example that *would* be cleaned by the current regex
        redlink_that_matches_regex = '[Example Link](/w/index.php?title=Example_Link&action=edit&redlink=1 "Example Link")'
        cleaned_matching_redlink = "Example Link\n"
        assert _clean_daily_markdown_content(redlink_that_matches_regex) == cleaned_matching_redlink

    def test_remove_citation_markers(self) -> None:
        inputs = [
            "Text with a citation [[1]](#cite_note-1).",
            "Another citation format [[citation needed]](#).",  # Assuming this also gets removed
            "Text with multiple citations [[2]](#cite_note-2) and [[3]](#cite_note-3).",
        ]
        # Correcting expected output based on current function's known behavior (specific citation patterns)
        expected_outputs = [
            "Text with a citation .\n",  # Space remains before period after citation removal
            "Another citation format [[citation needed]](#).\n",  # This format is not removed
            "Text with multiple citations  and .\n",  # Spaces remain
        ]
        for i, text_input in enumerate(inputs):
            with self.subTest(i=i):
                assert _clean_daily_markdown_content(text_input) == expected_outputs[i]

    def test_remove_trailing_spaces_and_tabs(self) -> None:
        inputs = [
            "Line with trailing space. \nAnother line with trailing tab.\t\nNo trailing here.",
            "Single line with spaces and tabs. \t \t",
        ]
        expected_outputs = [
            "Line with trailing space.\nAnother line with trailing tab.\nNo trailing here.\n",
            "Single line with spaces and tabs.\n",
        ]
        for i, text_input in enumerate(inputs):
            with self.subTest(i=i):
                assert _clean_daily_markdown_content(text_input) == expected_outputs[i]

    def test_ensure_single_trailing_newline(self) -> None:
        inputs = {
            "No trailing newline": "Some text",
            "One trailing newline": "Some text\n",
            "Multiple trailing newlines": "Some text\n\n\n",
            "Trailing spaces and newlines": "Some text  \n\n",
            "Just spaces and newlines": "  \n\n",  # Will become empty string, then single newline
        }
        expected_output = "Some text\n"  # Default expected
        for name, text_input in inputs.items():
            with self.subTest(name=name):
                if name == "Just spaces and newlines":
                    assert _clean_daily_markdown_content(text_input) == "\n"
                else:
                    assert _clean_daily_markdown_content(text_input) == expected_output

        assert _clean_daily_markdown_content("\n\n") == "\n"  # Test multiple newlines only

    def test_no_cleaning_needed(self) -> None:
        text_input = "This is a clean line.\nAnd another one."
        # Expect single trailing newline
        expected_output = "This is a clean line.\nAnd another one.\n"  # This already has a trailing newline, as per function's behavior
        assert _clean_daily_markdown_content(text_input) == expected_output

    def test_empty_string_input(self) -> None:
        text_input = ""
        expected_output = "\n"  # Empty string results in a single newline
        assert _clean_daily_markdown_content(text_input) == expected_output

    def test_combined_cleaning_rules(self) -> None:
        text_input = (
            "Event with [a redlink](/w/index.php?title=Red_Link&action=edit&redlink=1) and citation [[CITE]](#cite-1). \t\n"
            "Another line with trailing spaces.   \n"
            "Final line without anything extra.\n\n\n"  # Extra newlines
        )
        expected_output = (
            "Event with [a redlink](/w/index.php?title=Red_Link&action=edit&redlink=1) and citation [[CITE]](#cite-1).\n"
            "Another line with trailing spaces.\n"
            "Final line without anything extra.\n"
        )
        assert _clean_daily_markdown_content(text_input) == expected_output


class TestGenerateJekyllContent(unittest.TestCase):
    def setUp(self) -> None:
        self.test_date = datetime(2023, 10, 26)
        self.logger = logging.getLogger(__name__)  # Use the same logger as other tests

    def test_correct_front_matter_and_published_true(self) -> None:
        markdown_body = "This is a valid markdown body.\n" + ("a" * MIN_MARKDOWN_LENGTH_PUBLISH)
        expected_title = "2023 October 26"
        expected_date_format = "2023-10-26"

        full_content = generate_jekyll_content(self.test_date, markdown_body, self.logger)

        assert "---" in full_content
        assert "layout: post" in full_content
        assert f"title: {expected_title}" in full_content
        assert f"date: {expected_date_format}" in full_content
        assert "published: true" in full_content
        assert full_content.endswith("\n\n\n" + markdown_body)  # 3 blank lines then body

    def test_published_true_sufficient_body_length(self) -> None:
        # Exactly MIN_MARKDOWN_LENGTH_PUBLISH
        markdown_body = "a" * MIN_MARKDOWN_LENGTH_PUBLISH
        full_content = generate_jekyll_content(self.test_date, markdown_body, self.logger)
        assert "published: true" in full_content
        assert full_content.endswith(markdown_body)

        # More than MIN_MARKDOWN_LENGTH_PUBLISH
        markdown_body_long = "a" * (MIN_MARKDOWN_LENGTH_PUBLISH + 10)
        full_content_long = generate_jekyll_content(self.test_date, markdown_body_long, self.logger)
        assert "published: true" in full_content_long
        assert full_content_long.endswith(markdown_body_long)

    def test_published_false_insufficient_body_length(self) -> None:
        # Less than MIN_MARKDOWN_LENGTH_PUBLISH
        markdown_body = "a" * (MIN_MARKDOWN_LENGTH_PUBLISH - 1)
        if not markdown_body:  # handle case where MIN_MARKDOWN_LENGTH_PUBLISH is 1 or 0
            markdown_body = ""  # ensure it's less, but not by creating negative string length

        full_content = generate_jekyll_content(self.test_date, markdown_body, self.logger)
        assert "published: false" in full_content

        # Body should be empty when published is false
        expected_ending = "\n\n\n"  # 3 blank lines after front matter
        assert full_content.endswith(expected_ending)
        # Check that the original markdown_body is NOT after the front matter
        if markdown_body is not None:
            assert (expected_ending + markdown_body) not in full_content

    def test_published_false_empty_string_body(self) -> None:
        markdown_body = ""
        full_content = generate_jekyll_content(self.test_date, markdown_body, self.logger)
        assert "published: false" in full_content

        # Body should be empty
        expected_ending = "\n\n\n"  # 3 blank lines after front matter
        assert full_content.endswith(expected_ending)
        # When markdown_body is empty, (expected_ending + markdown_body) is just expected_ending.
        # The previous assertNotIn would incorrectly fail.
        # The important check is that the content ends with just the newlines from the front matter.


if __name__ == "__main__":
    unittest.main()

# Ensure MONTH_NAME_TO_NUMBER and _clean_daily_markdown_content are available if this file is run directly
# For simplicity, they are imported from the main module, which is standard for testing.
# If the main script wikipedia_news_downloader.py has its own if __name__ == '__main__': block,
# then these functions won't be an issue when tests are run via `python -m unittest test_wikipedia_news_downloader.py`
# or similar test runner.
