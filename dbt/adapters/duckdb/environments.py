import importlib.util
import os
import tempfile

import duckdb

from .credentials import DuckDBCredentials
from dbt.contracts.connection import AdapterResponse
from dbt.exceptions import DbtRuntimeError


class DuckDBCursorWrapper:
    def __init__(self, cursor):
        self._cursor = cursor

    # forward along all non-execute() methods/attribute look ups
    def __getattr__(self, name):
        return getattr(self._cursor, name)

    def execute(self, sql, bindings=None):
        try:
            if bindings is None:
                return self._cursor.execute(sql)
            else:
                return self._cursor.execute(sql, bindings)
        except RuntimeError as e:
            raise DbtRuntimeError(str(e))


class DuckDBConnectionWrapper:
    def __init__(self, env, cursor):
        self._env = env
        self._cursor = DuckDBCursorWrapper(cursor)

    def close(self):
        self._env.close(self._cursor)

    def cursor(self):
        return self._cursor


class Environment:
    def handle(self):
        # Extensions/settings need to be configured per cursor
        cursor = self.conn.cursor()
        for ext in self.creds.extensions or []:
            cursor.execute(f"LOAD '{ext}'")
        for key, value in self.creds.load_settings().items():
            # Okay to set these as strings because DuckDB will cast them
            # to the correct type
            cursor.execute(f"SET {key} = '{value}'")
        return cursor

    def close(self, cursor):
        cursor.close()

    def submit_python_job(self, handle, parsed_model: dict, compiled_code: str) -> AdapterResponse:
        raise NotImplementedError


class LocalEnvironment(Environment):
    def __init__(self, credentials: DuckDBCredentials):
        self.conn = duckdb.connect(credentials.path, read_only=False)
        self.creds = credentials
        # install any extensions on the connection
        if credentials.extensions is not None:
            for extension in credentials.extensions:
                self.conn.execute(f"INSTALL '{extension}'")

        # Attach any fsspec filesystems on the database
        if credentials.filesystems:
            import fsspec

            for spec in credentials.filesystems:
                curr = spec.copy()
                fsimpl = curr.pop("fs")
                fs = fsspec.filesystem(fsimpl, **curr)
                self.conn.register_filesystem(fs)

        # attach any databases that we will be using
        if credentials.attach:
            for attachment in credentials.attach:
                self.conn.execute(attachment.to_sql())
        self.handles = 0

    def handle(self):
        self.handles += 1
        # Extensions/settings need to be configured per cursor
        cursor = self.conn.cursor()
        for ext in self.creds.extensions or []:
            cursor.execute(f"LOAD '{ext}'")
        for key, value in self.creds.load_settings().items():
            # Okay to set these as strings because DuckDB will cast them
            # to the correct type
            cursor.execute(f"SET {key} = '{value}'")
        return DuckDBConnectionWrapper(self, cursor)

    def submit_python_job(self, handle, parsed_model: dict, compiled_code: str) -> AdapterResponse:
        con = handle.cursor()

        def load_df_function(table_name: str):
            """
            Currently con.table method dos not support fully qualified name - https://github.com/duckdb/duckdb/issues/5038

            Can be replaced by con.table, after it is fixed.
            """
            return con.query(f"select * from {table_name}")

        identifier = parsed_model["alias"]
        mod_file = tempfile.NamedTemporaryFile(suffix=".py", delete=False)
        mod_file.write(compiled_code.lstrip().encode("utf-8"))
        mod_file.close()
        try:
            spec = importlib.util.spec_from_file_location(identifier, mod_file.name)
            if not spec:
                raise DbtRuntimeError(
                    "Failed to load python model as module: {}".format(identifier)
                )
            module = importlib.util.module_from_spec(spec)
            if spec.loader:
                spec.loader.exec_module(module)
            else:
                raise DbtRuntimeError(
                    "Python module spec is missing loader: {}".format(identifier)
                )

            # Do the actual work to run the code here
            dbt = module.dbtObj(load_df_function)
            df = module.model(dbt, con)
            module.materialize(df, con)
        except Exception as err:
            raise DbtRuntimeError(f"Python model failed:\n" f"{err}")
        finally:
            os.unlink(mod_file.name)
        return AdapterResponse(_message="OK")

    def close(self, cursor):
        super().close(cursor)
        self.handles -= 1
        if self.conn and self.handles == 0 and self.creds.path != ":memory:":
            self.conn.close()
            self.conn = None

    def __del__(self):
        self.conn.close()
        self.conn = None


def create(creds: DuckDBCredentials) -> Environment:
    if creds.remote:
        from .buenavista import BVEnvironment

        return BVEnvironment(creds)
    else:
        return LocalEnvironment(creds)
