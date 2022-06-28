import functools
import inspect
from enum import Enum
from typing import Any, Type, TypeVar, Union
from unittest.mock import MagicMock

import pendulum
from airflow import macros
from airflow.models.baseoperator import BaseOperator
from airflow.models.dag import DAG
from airflow.models.taskinstance import TaskInstance
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.apache.hdfs.sensors.hdfs import HdfsSensor
from airflow.providers.apache.hive.operators.hive import HiveOperator
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import \
    KubernetesPodOperator

XCom = {}
TBaseOperator = TypeVar("TBaseOperator", bound=BaseOperator)


class AllowedOperators(Enum):
    BashOperator = BashOperator
    PythonOperator = PythonOperator
    HdfsSensor = HdfsSensor
    HiveOperator = HiveOperator
    KubernetesPodOperator = KubernetesPodOperator
    S3KeySensor = S3KeySensor

    def __call__(self, *args, **kwargs):
        return self.value(*args, **kwargs)


class TaskInstanceMock(MagicMock):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def _update(self, d, u):
        for k, v in u.items():
            if isinstance(v, dict):
                d[k] = self._update(d.get(k, {}), v)
            else:
                d[k] = v
        return d

    def xcom_push(self, key: str, value: Any, **kwargs):
        task_id = str(id(self.task_id))
        dag_id = str(id(self.dag_id))
        self._update(XCom, {dag_id: {task_id: {key: value}}})

    def xcom_pull(self, task_ids: str, dag_id: str, key: str):
        return XCom[dag_id][task_ids][key]


def make_kwargs(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        if "task_id" not in kwargs:
            operator = kwargs["operator"]
            if isinstance(operator, type):
                kwargs["task_id"] = f"execute_{operator.__name__}"
            else:
                kwargs["task_id"] = f"execute_{operator}"
        if "start_date" not in kwargs:
            kwargs["start_date"] = pendulum.now()
        return func(*args, **kwargs)
    return wrapper


class ExecuteAnyOperator(BaseOperator):
    @make_kwargs
    def __init__(self, operator: Union[str, Type[TBaseOperator]], *args, **kwargs):
        params = inspect.signature(BaseOperator).parameters
        base_kwargs = {k: kwargs[k] for k in params if k in kwargs}
        super().__init__(**base_kwargs)

        if isinstance(operator, type) and operator.__name__ in AllowedOperators.__members__:
            self.operator = operator
        elif isinstance(operator, str) and operator in AllowedOperators.__members__:
            self.operator = AllowedOperators[operator]
        else:
            raise NotImplementedError(f"Operator {operator} is not supported")

        self.dag = DAG(
            "dummy_dag",
            default_args={"owner": "airflow", "start_date": pendulum.today().subtract(days=1)},
            schedule_interval=None,
        )
        self.start_date = kwargs["start_date"]
        self.task = self.operator(**kwargs)
        self.context = self._generate_context()

    def _generate_context(self):
        task_instance = TaskInstanceMock(spec=TaskInstance)
        ds = self.start_date.to_date_string()
        ds_nodash = ds.replace("-", "")
        ts = self.start_date.isoformat()
        ts_nodash = self.start_date.strftime('%Y%m%dT%H%M%S')
        ts_nodash_with_tz = ts.replace("-", "").replace(":", "")

        return {
            'conf': None,
            'dag': self.dag,
            'dag_run': None,
            'data_interval_end': None,
            'data_interval_start': self.start_date,
            'ds': ds,
            'ds_nodash': ds_nodash,
            'inlets': None,
            'macros': macros,
            'outlets': None,
            'params': None,
            'prev_data_interval_start_success': None,
            'prev_data_interval_end_success': None,
            'prev_execution_date_success': None,
            'run_id': None,
            'task': self.task,
            'task_instance': task_instance,
            'task_instance_key_str': f"{self.dag.dag_id}__{self.task_id}__{ds_nodash}",
            'test_mode': False,
            'ti': task_instance,
            'ts': ts,
            'ts_nodash': ts_nodash,
            'ts_nodash_with_tz': ts_nodash_with_tz,
            'var': None,
            'conn': None,
        }

    def execute(self):
        return self.task.execute(context=self.context)
