import importlib
import json
import logging
import os
from json.decoder import JSONDecodeError
from typing import Union

from sqlalchemy.sql.expression import BinaryExpression, BindParameter

log = logging.getLogger(__name__)


def get_connection_from_secrets(conn_id: str):
    secrets_backends = []

    custom_secrets_backend = os.getenv("AIRFLOW__SECRETS__BACKEND")
    if custom_secrets_backend is not None:
        backend_module, backend_class = custom_secrets_backend.rsplit(".", 1)
        secrets_backend_module = importlib.import_module(backend_module)
        secrets_backend_cls = getattr(secrets_backend_module, backend_class)

        try:
            alternative_secrets_config_dict = json.loads(
                os.getenv("AIRFLOW__SECRETS__BACKEND_KWARGS", "{}")
            )
        except JSONDecodeError:
            alternative_secrets_config_dict = {}

        secrets_backends.append(secrets_backend_cls(**alternative_secrets_config_dict))

    env_secrets_backend_module = importlib.import_module(
        "execute_any_operator.utils.secrets_backend"
    )
    env_secrets_backend_cls = getattr(
        env_secrets_backend_module, "EnvironmentSecretsBackend"
    )
    secrets_backends.append(env_secrets_backend_cls())

    for secrets_backend in secrets_backends:
        try:
            connections = secrets_backend.get_connections(conn_id=conn_id)
            if connections:
                return connections
        except Exception:
            log.exception(
                "Unable to retrieve connection from secrets backend (%s). "
                "Checking subsequent secrets backend.",
                type(secrets_backend).__name__,
            )
    return []


class mockFilter:
    def __init__(self, _model, _filter):
        self._filter: BinaryExpression = _filter
        self._model = _model

    def _get_filter_value(self) -> Union[str, None]:
        if isinstance(self._filter.left, BindParameter):
            return self._filter.left.value
        elif isinstance(self._filter.right, BindParameter):
            return self._filter.right.value

    def first(self):
        _all = self.all()
        if len(_all) > 0:
            return _all[0]
        return None

    def all(self):
        filter_value: Union[str, None] = self._get_filter_value()
        if (
            isinstance(self._model, str)
            and self._model == "connection"
            or self._model.__tablename__ == "connection"
        ):
            return get_connection_from_secrets(conn_id=filter_value)
        return []

    def count(self):
        return len(self.all())


class mockQuery:
    def __init__(self, _model):
        self._model = _model

    def filter(self, _filter):
        return mockFilter(self._model, _filter)

    def all(self):
        if (
            isinstance(self._model, str)
            and self._model == "connection"
            or self._model.__tablename__ == "connection"
        ):
            return get_connection_from_secrets(conn_id=None)
        return []

    def count(self):
        return len(self.all())


class mockSession:
    def __init__(self, *args, **kwargs):
        self.dirty = []

    def query(self, model):
        return mockQuery(model)

    def flush(self):
        pass

    def remove(self):
        pass

    def close(self):
        pass

    def rollback(self):
        pass

    def commit(self):
        pass

    def expunge_all(self):
        pass

    def __enter__(self):
        print("enter session")
        return self

    def __exit__(self):
        print("exit session")

    def __call__(self):
        return self
