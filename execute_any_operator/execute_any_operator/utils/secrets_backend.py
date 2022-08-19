import importlib
import os
import re
from typing import Any, List

from airflow.secrets.environment_variables import EnvironmentVariablesBackend
from airflow.secrets.local_filesystem import LocalFilesystemBackend


class LocalSecretsBackend(LocalFilesystemBackend):
    def get_connections(self, conn_id: str = None) -> List[Any]:
        if conn_id is None:
            return list(self._local_connections.values())
        if "%" in conn_id or "*" in conn_id:
            filter_regex = f"^{conn_id.replace('%', '.*').replace('_', '.')}$"
            connections = {
                k.lower(): v
                for k, v in self._local_connections.items()
                if re.search(filter_regex, k)
            }
            return list(connections.values())
        return super().get_connections(conn_id=conn_id)


class EnvironmentSecretsBackend(EnvironmentVariablesBackend):
    def get_connections(self, conn_id: str = None) -> List["Connection"]:
        models = importlib.import_module("airflow.models")
        Connection = getattr(models, "Connection")
        connections = {
            k.replace("AIRFLOW_CONN_", "").lower(): v
            for k, v in os.environ.items()
            if re.search("^AIRFLOW_CONN_.*$", k)
        }

        if conn_id is None:
            return [Connection(conn_id=k, uri=v) for k, v in connections.items()]
        if "%" in conn_id or "*" in conn_id:
            filter_regex = (
                f"^AIRFLOW_CONN_{conn_id.upper().replace('%', '.*').replace('_', '.')}$"
            )
            connections = {
                k.replace("AIRFLOW_CONN_", "").lower(): v
                for k, v in os.environ.items()
                if re.search(filter_regex, k)
            }
            return [Connection(conn_id=k, uri=v) for k, v in connections.items()]
        return super().get_connections(conn_id=conn_id)
