import json

import psycopg2

from .credentials import DuckDBCredentials
from .environments import Environment
from dbt.contracts.connection import AdapterResponse


class BVEnvironment(Environment):
    def __init__(self, credentials: DuckDBCredentials):
        conn = psycopg2.connect(credentials.uri)

        # install any extensions on the connection
        if credentials.extensions is not None:
            for extension in credentials.extensions:
                conn.execute(f"INSTALL '{extension}'")

        # Attach any fsspec filesystems on the database
        if credentials.filesystems:
            # TODO: extension for registering these
            pass

        # attach any databases that we will be using
        if credentials.attach:
            for attachment in credentials.attach:
                conn.execute(attachment.to_sql())
        super().__init__(conn, credentials)

    def submit_python_job(
        self, handle, parsed_model: dict, compiled_code: str
    ) -> AdapterResponse:
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
