from datetime import datetime

from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.exceptions import AirflowException

from ..hooks.stitch_hook import StitchHook


class StitchRunSourceOperator(BaseOperator): 
    """
    This class serves to trigger stitch tasks and monitor them for success/failure.
    """
    ui_color = '#00cdcd'
    
    @apply_defaults
    def __init__(self, source_id: str=None, client_id: str=None, conn_id: str=None, *args, **kwargs) -> None:
        """
        Initialize a new instance of StitchRunSourceOperator.

        :param source_id: The ID of the source to run.
        :param client_id: The client ID to use.
        :param conn_id: The ID of the connection to use.
        """
        super().__init__(*args, **kwargs)

        if source_id is None:
            raise AirflowException('source_id is required')
        if client_id is None:
            raise AirflowException('client_id is required')
        if conn_id is None:
            raise AirflowException('conn_id is required')

        self.source_id = source_id
        self.client_id = client_id
        self.conn_id = conn_id

    def execute(self, context):
        self.stitch_hook = StitchHook(conn_id=self.conn_id)
        self.stitch_hook._get_credentials()
        self.log.info(f'Starting extraction: source_id = {self.source_id}')
        self.stitch_hook._trigger_extraction(source_id=self.source_id, client_id=self.client_id)


class StitchRunAndMonitorSourceOperator(StitchRunSourceOperator): 
    @apply_defaults
    def __init__(self, sleep_time=300, timeout=86400, *args, **kwargs) -> None:
        """
        Initialize a new instance of StitchRunAndMonitorSourceOperator.

        :param sleep_time: The time to sleep between checks.
        :param timeout: The maximum time to wait for the source to finish running.
        """
        super().__init__(*args, **kwargs)

        self.sleep_time = sleep_time
        self.timeout = timeout

    def execute(self, context):
        tic = datetime.now()

        super().execute(context)

        self.log.info(f'Monitoring source: source_id = {self.source_id}')
        self.stitch_hook._monitor_extraction(sleep_time=self.sleep_time, timeout=self.timeout, source_id=self.source_id, client_id=self.client_id, start_time=tic)
