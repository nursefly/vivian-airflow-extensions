import s3fs
from airflow.hooks.base import BaseHook
from airflow.utils.decorators import apply_defaults
from airflow.exceptions import AirflowException


class S3BookmarkHook(BaseHook):
    """
    This class interacts with S3 to get and save bookmarks.
    """
    template_fields = ['bookmark_s3_key']

    @apply_defaults
    def __init__(self, bookmark_s3_key: str=None, incremental_key_type: str=None, *args, **kwargs) -> None:
        """
        Initialize a new instance of S3BookmarkHook.

        :param bookmark_s3_key: The S3 key where the bookmark is stored.
        :param incremental_key_type: The type of the incremental key.
        """
        super().__init__(*args, **kwargs)

        if bookmark_s3_key is None:
            raise AirflowException('bookmark_s3_key is required')
        if incremental_key_type is None:
            raise AirflowException('incremental_key_type is required')
        
        self.bookmark_s3_key = bookmark_s3_key
        self.incremental_key_type = incremental_key_type   

    def _get_latest_bookmark(self):
        """
        Get the latest bookmark from S3.

        :return: The latest bookmark.
        """
        s3 = s3fs.S3FileSystem(anon=False)

        try:
            with s3.open(self.bookmark_s3_key, 'r') as f:
                key = f.read().strip()
            if self.incremental_key_type == 'timestamp':
                key = f"'{key}'" 
            self.log.info(f'Read {key} as the latest bookmark from {self.bookmark_s3_key}')
        except FileNotFoundError:
            if self.incremental_key_type == 'int':
                key = 0
            elif self.incremental_key_type == 'timestamp':
                key = "'1970-01-01 00:00:00'"
            self.log.info(f'No bookmark file found at {self.bookmark_s3_key}! Defaulting to {key}')

        return key  

    def _save_next_bookmark(self, bookmark):
        """
        Save the next bookmark to S3.

        :param bookmark: The bookmark to save.
        """
        if bookmark is None:
            self.log.info('Bookmark is None, not saving it')
            return
        else:
            bookmark = bookmark.strftime('%Y-%m-%d %H:%M:%S')

        s3 = s3fs.S3FileSystem(anon=False)

        with s3.open(self.bookmark_s3_key, 'w') as f:
            f.write(bookmark)
        self.log.info(f'Wrote {bookmark} as latest bookmark to {self.bookmark_s3_key}')
