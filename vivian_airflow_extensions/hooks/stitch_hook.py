import json
import requests
import time
from datetime import datetime

from airflow.exceptions import AirflowException
from airflow.utils.decorators import apply_defaults
from airflow.hooks.base import BaseHook


class StitchHook(BaseHook):
    @apply_defaults
    def __init__(self, conn_id: str, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

        if conn_id is None:
            raise AirflowException('conn_id is required')
        
        self.conn_id = conn_id

    def _get_response(self, url: str, method: str) -> dict:
        response = requests.request(method, url, headers=self.headers)
        response.raise_for_status()
        response_data = response.text
        dict_data = json.loads(response_data)

        return dict_data
    
    def get_credentials(self) -> None:
        conn = self.get_connection(conn_id=self.conn_id)
        self.host = conn.host
        self.headers = conn.extra_dejson
    
    def trigger_extraction(self, source_id: str, client_id: str) -> dict:
        self.get_credentials()
        url = (f'{self.host}/sources/{source_id}/sync')
        dict_data = self._get_response(url, 'POST')
        error = dict_data.get('error', None)
        job_name = dict_data.get('job_name', None)

        # check if process is already running (still returns 200 so need separate check)
        if error is not None:
            raise AirflowException(f'Error: type = {error.get("type", None)}, message = {error.get("message", None)}')
        
        if job_name is None:
            raise AirflowException(f'Error: the API did not return a job_name, response: {dict_data}')
        else:
            self.log.info(f'Extraction triggered: source_id = {source_id}, job_name = {job_name}, integration url = https://app.stitchdata.com/client/{client_id}/pipeline/v2/sources/{source_id}/')

    def monitor_extraction(self, source_id: str, client_id: str, sleep_time=300, timeout=86400, start_time: datetime=datetime.now()) -> None:
        time.sleep(30) # let the job actually trigger
        url = (f'{self.host}/{client_id}/extractions')
        id_found = False

        while (datetime.now() - start_time).seconds < timeout:
            dict_data = self._get_response(url, 'GET')

            for item in dict_data['data']:
                if str(item['source_id']) == source_id:
                    self.log.info(f'Extraction status object: {item}')
                    id_found = True
                    last_extraction_completion_time = datetime.strptime(item['completion_time'],'%Y-%m-%dT%H:%M:%SZ')

                    if last_extraction_completion_time < start_time:
                        self.log.info(f'Waiting for all extractions to complete: source_id = {source_id}')
                        time.sleep(sleep_time)
                        url = (f'{self.host}/{client_id}/extractions')   
                    elif item['tap_exit_status'] != 0 and item['tap_exit_status'] is not None:
                        raise AirflowException(f'Error: source_id = {source_id} extraction failed')
                    else:
                        exec_time = (datetime.now() - start_time).seconds
                        self.log.info(f'Extraction succeeded in {exec_time} seconds')
                        return 
                    break

            if 'next' in dict_data['links'] and not id_found:
                next = dict_data['links']['next']
                url = (f'https://api.stitchdata.com{next}')
            elif not id_found:
                raise AirflowException(f'Error: source_id = {source_id} not found in response')
        
        raise AirflowException(f'Error: source_id = {source_id} timed out')
