import unittest
from datetime import datetime
from unittest.mock import patch, MagicMock
import argparse

# Assuming wikipedia_news_downloader.py is in the same directory or accessible in PYTHONPATH
from wikipedia_news_downloader import parse_arguments, main, MONTH_NAME_TO_NUMBER

class TestWikipediaNewsDownloader(unittest.TestCase):

    def test_parse_arguments_valid_dates(self):
        """Test parsing valid start and end dates."""
        args = parse_arguments(['--start-date', '2023-01-01', '--end-date', '2023-01-31'])
        self.assertEqual(args.start_date, datetime(2023, 1, 1))
        self.assertEqual(args.end_date, datetime(2023, 1, 31))

    def test_parse_arguments_invalid_date_format(self):
        """Test parsing an invalid date format."""
        # Argparse calls sys.exit(2) on error, which raises SystemExit.
        # We need to catch that.
        with self.assertRaises(SystemExit):
            with patch('sys.stderr', MagicMock()): # Suppress argparse error message to keep test output clean
                 parse_arguments(['--start-date', '2023/01/01'])


    def test_parse_arguments_invalid_date_value(self):
        """Test parsing an invalid date value."""
        # Argparse calls sys.exit(2) on error.
        with self.assertRaises(SystemExit):
            with patch('sys.stderr', MagicMock()): # Suppress argparse error message
                parse_arguments(['--start-date', '2023-13-01']) # Invalid month

    def test_parse_arguments_only_start_date(self):
        """Test parsing only start date."""
        args = parse_arguments(['--start-date', '2023-02-15'])
        self.assertEqual(args.start_date, datetime(2023, 2, 15))
        self.assertIsNone(args.end_date)

    def test_parse_arguments_only_end_date(self):
        """Test parsing only end date."""
        args = parse_arguments(['--end-date', '2023-03-20'])
        self.assertEqual(args.end_date, datetime(2023, 3, 20))
        self.assertIsNone(args.start_date)

    def test_parse_arguments_no_dates(self):
        """Test parsing with no date arguments."""
        args = parse_arguments([])
        self.assertIsNone(args.start_date)
        self.assertIsNone(args.end_date)

    @patch('wikipedia_news_downloader.setup_logging')
    @patch('wikipedia_news_downloader.Path')
    @patch('wikipedia_news_downloader.queue.Queue')
    @patch('wikipedia_news_downloader.threading.Thread')
    def test_main_no_date_args(self, mock_thread, mock_queue_class, mock_path, mock_setup_logging):
        """Test main with no date arguments, expecting default date range."""
        mock_logger = MagicMock()
        mock_setup_logging.return_value = mock_logger
        mock_queue_instance = MagicMock()
        mock_queue_class.return_value = mock_queue_instance

        # Mock datetime.now() to control the default end date
        # Also need to mock datetime.datetime itself for when it's called directly
        with patch('wikipedia_news_downloader.datetime') as mock_dt_module:
            mock_dt_module.now.return_value = datetime(2024, 3, 15) # Current date for test
            mock_dt_module.strptime = datetime.strptime # Keep strptime working for arg parsing if it were used inside main

            # This is crucial: when datetime(Y,M,D) is called, it should return a real datetime object
            mock_dt_module.side_effect = lambda *args, **kw: datetime(*args, **kw) if args else mock_dt_module.now()


            main(output_dir_str="test_output", verbose=False, num_workers=1,
                 local_html_files_list=None, start_date_arg=None, end_date_arg=None, logger=mock_logger)

        # Expected default start: 2024-01-01, default end: 2024-03-01 (first of current month)
        # We expect Jan, Feb, Mar of 2024
        self.assertEqual(mock_queue_instance.put.call_count, 3)
        calls = mock_queue_instance.put.call_args_list
        expected_dates = [
            datetime(2024, 1, 1),
            datetime(2024, 2, 1),
            datetime(2024, 3, 1)
        ]
        actual_dates = sorted([call[0][0][1] for call in calls])
        self.assertEqual(actual_dates, expected_dates)

    @patch('wikipedia_news_downloader.setup_logging')
    @patch('wikipedia_news_downloader.Path')
    @patch('wikipedia_news_downloader.queue.Queue')
    @patch('wikipedia_news_downloader.threading.Thread')
    def test_main_with_start_date(self, mock_thread, mock_queue_class, mock_path, mock_setup_logging):
        """Test main with only start_date argument."""
        mock_logger = MagicMock()
        mock_setup_logging.return_value = mock_logger
        mock_queue_instance = MagicMock()
        mock_queue_class.return_value = mock_queue_instance

        start_date = datetime(2024, 2, 1)
        with patch('wikipedia_news_downloader.datetime') as mock_dt_module:
            mock_dt_module.now.return_value = datetime(2024, 3, 15)
            mock_dt_module.strptime = datetime.strptime
            mock_dt_module.side_effect = lambda *args, **kw: datetime(*args, **kw) if args else mock_dt_module.now()


            main(output_dir_str="test_output", verbose=False, num_workers=1,
                 local_html_files_list=None, start_date_arg=start_date, end_date_arg=None, logger=mock_logger)

        # Expected: Feb 2024, Mar 2024
        self.assertEqual(mock_queue_instance.put.call_count, 2)
        calls = mock_queue_instance.put.call_args_list
        expected_dates = [datetime(2024, 2, 1), datetime(2024, 3, 1)]
        actual_dates = sorted([call[0][0][1] for call in calls])
        self.assertEqual(actual_dates, expected_dates)

    @patch('wikipedia_news_downloader.setup_logging')
    @patch('wikipedia_news_downloader.Path')
    @patch('wikipedia_news_downloader.queue.Queue')
    @patch('wikipedia_news_downloader.threading.Thread')
    def test_main_with_end_date(self, mock_thread, mock_queue_class, mock_path, mock_setup_logging):
        """Test main with only end_date argument."""
        mock_logger = MagicMock()
        mock_setup_logging.return_value = mock_logger
        mock_queue_instance = MagicMock()
        mock_queue_class.return_value = mock_queue_instance

        end_date = datetime(2024, 2, 1)
        with patch('wikipedia_news_downloader.datetime') as mock_dt_module:
            mock_dt_module.now.return_value = datetime(2024, 3, 15)
            mock_dt_module.strptime = datetime.strptime
            mock_dt_module.side_effect = lambda *args, **kw: datetime(*args, **kw) if args else mock_dt_module.now()


            main(output_dir_str="test_output", verbose=False, num_workers=1,
                 local_html_files_list=None, start_date_arg=None, end_date_arg=end_date, logger=mock_logger)

        # Expected: Jan 2024, Feb 2024 (default start is 2024-01-01)
        self.assertEqual(mock_queue_instance.put.call_count, 2)
        calls = mock_queue_instance.put.call_args_list
        expected_dates = [datetime(2024, 1, 1), datetime(2024, 2, 1)]
        actual_dates = sorted([call[0][0][1] for call in calls])
        self.assertEqual(actual_dates, expected_dates)

    @patch('wikipedia_news_downloader.setup_logging')
    @patch('wikipedia_news_downloader.Path')
    @patch('wikipedia_news_downloader.queue.Queue')
    @patch('wikipedia_news_downloader.threading.Thread')
    def test_main_with_start_and_end_date(self, mock_thread, mock_queue_class, mock_path, mock_setup_logging):
        """Test main with both start_date and end_date arguments."""
        mock_logger = MagicMock()
        mock_setup_logging.return_value = mock_logger
        mock_queue_instance = MagicMock()
        mock_queue_class.return_value = mock_queue_instance

        start_date = datetime(2023, 11, 1)
        end_date = datetime(2024, 1, 1)
        with patch('wikipedia_news_downloader.datetime') as mock_dt_module:
            mock_dt_module.now.return_value = datetime(2024, 3, 15)
            mock_dt_module.strptime = datetime.strptime
            mock_dt_module.side_effect = lambda *args, **kw: datetime(*args, **kw) if args else mock_dt_module.now()


            main(output_dir_str="test_output", verbose=False, num_workers=1,
                 local_html_files_list=None, start_date_arg=start_date, end_date_arg=end_date, logger=mock_logger)

        # Expected: Nov 2023, Dec 2023, Jan 2024
        self.assertEqual(mock_queue_instance.put.call_count, 3)
        calls = mock_queue_instance.put.call_args_list
        expected_dates = [datetime(2023, 11, 1), datetime(2023, 12, 1), datetime(2024, 1, 1)]
        actual_dates = sorted([call[0][0][1] for call in calls])
        self.assertEqual(actual_dates, expected_dates)

    @patch('wikipedia_news_downloader.setup_logging')
    @patch('wikipedia_news_downloader.Path')
    @patch('wikipedia_news_downloader.queue.Queue')
    @patch('wikipedia_news_downloader.threading.Thread')
    def test_main_start_after_end_date(self, mock_thread, mock_queue_class, mock_path, mock_setup_logging):
        """Test main with start_date after end_date."""
        mock_logger = MagicMock()
        mock_setup_logging.return_value = mock_logger
        mock_queue_instance = MagicMock()
        mock_queue_class.return_value = mock_queue_instance

        start_date = datetime(2024, 2, 1)
        end_date = datetime(2024, 1, 1) # Start is after end
        with patch('wikipedia_news_downloader.datetime') as mock_dt_module:
            mock_dt_module.now.return_value = datetime(2024, 3, 15)
            mock_dt_module.strptime = datetime.strptime
            mock_dt_module.side_effect = lambda *args, **kw: datetime(*args, **kw) if args else mock_dt_module.now()

            main(output_dir_str="test_output", verbose=False, num_workers=1,
                 local_html_files_list=None, start_date_arg=start_date, end_date_arg=end_date, logger=mock_logger)

        # Expected: No items to process
        self.assertEqual(mock_queue_instance.put.call_count, 0)

if __name__ == '__main__':
    # Running tests via unittest.main() is standard.
    # Adding argv and exit=False is useful for running in some environments (like notebooks or scripts)
    # where sys.argv might be different or you don't want the script to exit.
    unittest.main(argv=['first-arg-is-ignored'], exit=False)
