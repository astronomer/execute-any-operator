import functools
import inspect
import logging
from contextlib import ExitStack
from enum import Enum
from typing import Any, Type, TypeVar, Union
from unittest.mock import MagicMock

import pendulum
from execute_any_operator.utils.mock import context_patches

with ExitStack() as stack:
    managers = [stack.enter_context(patch) for patch in context_patches]

    import remote_bash_operator.operator
    from airflow import macros
    from airflow.models.baseoperator import BaseOperator
    from airflow.models.dag import DAG
    from airflow.models.taskinstance import XCOM_RETURN_KEY, TaskInstance
    from airflow.operators.bash import BashOperator
    from airflow.operators.python import PythonOperator
    from airflow.providers.amazon.aws.sensors.s3_key import S3KeySensor
    from airflow.providers.apache.hdfs.sensors.hdfs import HdfsSensor
    from airflow.providers.apache.hive.operators.hive import HiveOperator
    from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import \
        KubernetesPodOperator
    from airflow.providers.http.operators.http import SimpleHttpOperator
    from arrow_hdfs_sensor.sensor import ArrowHdfsSensor
    from remote_bash_operator.operator import RemoteBashOperator

TBaseOperator = TypeVar("TBaseOperator", bound=BaseOperator)

log = logging.getLogger(__name__)


class AllowedOperators(Enum):
    BashOperator = BashOperator
    PythonOperator = PythonOperator
    HdfsSensor = HdfsSensor
    HiveOperator = HiveOperator
    KubernetesPodOperator = KubernetesPodOperator
    S3KeySensor = S3KeySensor
    ArrowHdfsSensor = ArrowHdfsSensor
    RemoteBashOperator = RemoteBashOperator
    SimpleHttpOperator = SimpleHttpOperator

    def __call__(self, *args, **kwargs):
        return self.value(*args, **kwargs)


# TODO: remove these mocks when maven is installed
remote_bash_operator.operator.maven_install = MagicMock()
remote_bash_operator.operator.verify_submitter = MagicMock()


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

        if (
            isinstance(operator, type)
            and operator.__name__ in AllowedOperators.__members__
        ):
            self.operator = operator
        elif isinstance(operator, str) and operator in AllowedOperators.__members__:
            self.operator = AllowedOperators[operator]
        else:
            raise NotImplementedError(f"Operator {operator} is not supported")

        self.dag = DAG(
            "dummy_dag",
            default_args={
                "owner": "airflow",
                "start_date": pendulum.today().subtract(days=1),
            },
            schedule_interval=None,
        )
        self.start_date = kwargs["start_date"]
        self.task = self.operator(**kwargs)
        self.task._log = log
        self.context = self._generate_context()

    def _generate_context(self):
        ds = self.start_date.to_date_string()
        ds_nodash = ds.replace("-", "")
        ts = self.start_date.isoformat()
        ts_nodash = self.start_date.strftime("%Y%m%dT%H%M%S")
        ts_nodash_with_tz = ts.replace("-", "").replace(":", "")
        task_instance = TaskInstance(task=self.task, execution_date=self.start_date, run_id=f"cli__{ts_nodash}")

        return {
            "conf": None,
            "dag": self.dag,
            "dag_run": None,
            "data_interval_end": None,
            "data_interval_start": self.start_date,
            "ds": ds,
            "ds_nodash": ds_nodash,
            "inlets": None,
            "macros": macros,
            "outlets": None,
            "params": None,
            "prev_data_interval_start_success": None,
            "prev_data_interval_end_success": None,
            "prev_execution_date_success": None,
            "run_id": None,
            "task": self.task,
            "task_instance": task_instance,
            "task_instance_key_str": f"{self.dag.dag_id}__{self.task_id}__{ds_nodash}",
            "test_mode": False,
            "ti": task_instance,
            "ts": ts,
            "ts_nodash": ts_nodash,
            "ts_nodash_with_tz": ts_nodash_with_tz,
            "var": None,
            "conn": None,
        }

    def pre_execute(self):
        return self.task.pre_execute(context=self.context)

    def post_execute(self, context: Any, result: Any = None):
        return self.task.post_execute(context, result)

    def execute(self):
        self.log.info(f"ExecuteAnyOperator is executing {self.task}")
        result = self.task.execute(context=self.context)
        if self.task.do_xcom_push and result is not None:
            self.task.xcom_push(self.context, key=XCOM_RETURN_KEY, value=result)
        return result
