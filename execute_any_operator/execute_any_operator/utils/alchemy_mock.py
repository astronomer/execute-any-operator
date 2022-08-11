import importlib
import os
import re
from typing import Union

from sqlalchemy.sql.expression import BinaryExpression, BindParameter


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
        models = importlib.import_module('airflow.models')
        if isinstance(self._model, str) and self._model == "connection" or self._model.__tablename__ == "connection":
            Connection = getattr(models, "Connection")
            filter_value: Union[str, None] = self._get_filter_value()
            if filter_value:
                filter_regex = f"^AIRFLOW_CONN_{filter_value.upper().replace('%', '.*').replace('_', '.')}$"
                connections = {k.replace("AIRFLOW_CONN_", "").lower(): v for k, v in os.environ.items() if re.search(filter_regex, k)}
                return [Connection(conn_id=k, uri=v) for k, v in connections.items()]
        return []

    def count(self):
        return len(self.all())


class mockQuery:
    def __init__(self, _model):
        self._model = _model

    def filter(self, _filter):
        return mockFilter(self._model, _filter)

    def all(self):
        models = importlib.import_module('airflow.models')
        if isinstance(self._model, str) and self._model == "connection" or self._model.__tablename__ == "connection":
            Connection = getattr(models, "Connection")
            filter_regex = "^AIRFLOW_CONN_.+$"
            connections = {k.replace("AIRFLOW_CONN_", "").lower(): v for k, v in os.environ.items() if re.search(filter_regex, k)}
            return [Connection(conn_id=k, uri=v) for k, v in connections.items()]
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

    def __enter__(self):
        print("enter session")
        return self

    def __exit__(self):
        print("exit session")

    def __call__(self):
        return self
