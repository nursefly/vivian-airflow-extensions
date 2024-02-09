import unittest
from unittest.mock import patch, MagicMock
from datetime import datetime

from airflow.exceptions import AirflowException

from vivian_airflow_extensions.hooks.s3_bookmark_hook import S3BookmarkHook

class TestS3BookmarkHook(unittest.TestCase):
    def test_init(self):
        with self.assertRaises(AirflowException):
            S3BookmarkHook()

        with self.assertRaises(AirflowException):
            S3BookmarkHook(bookmark_s3_key='test_key')

        hook = S3BookmarkHook(bookmark_s3_key='test_key', incremental_key_type='int')
        self.assertEqual(hook.bookmark_s3_key, 'test_key')
        self.assertEqual(hook.incremental_key_type, 'int')

    @patch('s3fs.S3FileSystem')
    def test_get_latest_bookmark(self, mock_s3fs):
        mock_file = MagicMock()
        mock_file.read.return_value = '123\n'
        mock_s3fs().open.return_value.__enter__.return_value = mock_file

        hook = S3BookmarkHook(bookmark_s3_key='test_key', incremental_key_type='int')
        bookmark = hook.get_latest_bookmark()

        self.assertEqual(bookmark, '123')

    @patch('s3fs.S3FileSystem')
    def test_get_latest_bookmark_file_not_found(self, mock_s3fs):
        mock_s3fs().open.side_effect = FileNotFoundError

        hook = S3BookmarkHook(bookmark_s3_key='test_key', incremental_key_type='int')
        bookmark = hook.get_latest_bookmark()

        self.assertEqual(bookmark, 0)

    @patch('s3fs.S3FileSystem')
    def test_save_next_bookmark(self, mock_s3fs):
        mock_file = MagicMock()
        mock_s3fs().open.return_value.__enter__.return_value = mock_file

        hook = S3BookmarkHook(bookmark_s3_key='test_key', incremental_key_type='int')
        hook.save_next_bookmark(datetime(2022, 1, 1))

        mock_file.write.assert_called_once_with('2022-01-01 00:00:00')

    @patch('s3fs.S3FileSystem')
    def test_save_next_bookmark_none(self, mock_s3fs):
        mock_file = MagicMock()
        mock_s3fs().open.return_value.__enter__.return_value = mock_file

        hook = S3BookmarkHook(bookmark_s3_key='test_key', incremental_key_type='int')
        hook.save_next_bookmark(None)

        mock_file.write.assert_not_called()

if __name__ == '__main__':
    unittest.main()