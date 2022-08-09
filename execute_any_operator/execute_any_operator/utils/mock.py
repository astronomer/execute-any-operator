import os
from typing import Any, Iterable, Optional, Union
from unittest.mock import MagicMock, patch

from airflow import settings
from airflow.models.taskinstance import TaskInstance
from airflow.models.variable import Variable
from airflow.models.xcom import XCom
from airflow.utils.helpers import is_container


class _Variable(Variable):
    @classmethod
    def get(
        cls, key: str, default_var: Any = ..., deserialize_json: bool = False
    ) -> Any:
        return os.getenv(key.upper())


class _TaskInstance(TaskInstance):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def xcom_push(self, key: str, value: Any, **kwargs) -> None:
        XCom.set(
            key=key,
            value=value,
            task_id=self.task_id,
            dag_id=self.dag_id,
        )

    def xcom_pull(self, task_ids: Optional[Union[str, Iterable[str]]] = None, dag_id: Optional[str] = None, key: str = ..., **kwargs) -> Any:
        if is_container(task_ids):
            return XCom.get_many(
                key=key,
                task_ids=task_ids,
                dag_ids=dag_id,
            )

        return XCom.get_one(
            key=key,
            task_id=task_ids,
            dag_id=dag_id,
        )


class _Session(MagicMock):
    def query(self, *args, **kwargs):
        print("querying")

    def __enter__():
        pass

    def __exit__():
        pass


context_patches = (
    patch("airflow.models.Variable", _Variable),
    patch("airflow.models.variable.Variable", _Variable),
    patch("airflow.models.TaskInstance", _TaskInstance),
    patch("airflow.models.taskinstance.TaskInstance", _TaskInstance),
    patch("airflow.utils.session.create_session", return_value=_Session(spec=settings.SASession))
)
