import os
from typing import Any
from unittest.mock import patch

from airflow.models.variable import Variable


class _PatchedVariable(Variable):
    @classmethod
    def get(
        cls, key: str, default_var: Any = ..., deserialize_json: bool = False
    ) -> Any:
        return os.getenv(key.upper())


context_patches = (patch("airflow.models.Variable", _PatchedVariable),)
