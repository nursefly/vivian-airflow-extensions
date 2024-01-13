import logging
from datetime import datetime

from airflow.sensors.base_sensor_operator import BaseSensorOperator
from airflow.utils.decorators import apply_defaults
from airflow.exceptions import AirflowException

from ..hooks.stitch_hook import StitchHook


class StitchMonitorSourceSensor(BaseSensorOperator):
    """
    This class allows for monitoring of Stitch jobs.
    """
    @apply_defaults
    def __init__(self, source_id: str=None, client_id: str=None, conn_id: str=None, sleep_time: int=300, timeout: int=86400, start_time: datetime=datetime.now(), *args, **kwargs) -> None:
        """
        Initialize a new instance of StitchSensor.

        :param stitch_conn_id: The ID of the connection to use.
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
        self.sleep_time = sleep_time
        self.timeout = timeout
        self.start_time = start_time

    def poke(self, context):
        """
        Function called in a loop until it returns True or the task times out.

        :param context: The execution context.
        :return: Whether the condition is satisfied.
        """
        self.stitch_hook = StitchHook(conn_id=self.conn_id)

        tic = datetime.now()

        self.stitch_hook._get_credentials()

        logging.info(f'Monitoring source: source_id = {self.source_id}')
        self.stitch_hook._monitor_extraction(sleep_time=self.sleep_time, timeout=self.timeout, source_id=self.source_id, client_id=self.client_id, start_time=tic)
