import json

import psycopg2

from . import credentials
from .environments import Environment
from dbt.contracts.connection import AdapterResponse


class BVEnvironment(Environment):
    @classmethod
    def _get_conn(cls, dbname: str, remote: credentials.Remote):
        return psycopg2.connect(
            dbname=dbname,
            user=remote.user,
            host=remote.host,
            port=remote.port,
        )

    def __init__(self, credentials: credentials.DuckDBCredentials):
        self.creds = credentials
        if not self.creds.remote:
            raise Exception("BVConnection only works with a remote host")

    def handle(self):
        # Extensions/settings need to be configured per cursor
        conn = self._get_conn(self.creds.database, self.creds.remote)
        cursor = conn.cursor()
        for ext in self.creds.extensions or []:
            cursor.execute(f"LOAD '{ext}'")
        for key, value in self.creds.load_settings().items():
            # Okay to set these as strings because DuckDB will cast them
            # to the correct type
            cursor.execute(f"SET {key} = '{value}'")
        cursor.close()
        return conn

    def submit_python_job(self, handle, parsed_model: dict, compiled_code: str) -> AdapterResponse:
        identifier = parsed_model["alias"]
        payload = {
            "method": "dbt_python_job",
            "params": {
                "module_name": identifier,
                "module_definition": compiled_code,
            },
        }
        # TODO: handle errors here
        handle.cursor().execute(json.dumps(payload))
        return AdapterResponse(_message="OK")
