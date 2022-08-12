import functools
import importlib
import inspect
import logging
from contextlib import ExitStack
from typing import Any, TypeVar
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

TBaseOperator = TypeVar("TBaseOperator", bound=BaseOperator)

log = logging.getLogger(__name__)


# TODO: remove these mocks when maven is installed
remote_bash_operator.operator.maven_install = MagicMock()
remote_bash_operator.operator.verify_submitter = MagicMock()


def make_kwargs(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        operator = kwargs["operator"]
        try:
            mod_name, op_name = operator.split(":", 1)
        except ValueError:
            raise ValueError(
                f"Operator string {operator} improperly formatted, must be in module notation (my.module:OperatorClass)"
            )

        kwargs["mod_name"] = mod_name
        kwargs["op_name"] = op_name
        if "task_id" not in kwargs:
            kwargs["task_id"] = f"execute_{op_name}"
        if "start_date" not in kwargs:
            kwargs["start_date"] = pendulum.now()
        return func(*args, **kwargs)

    return wrapper


class ExecuteAnyOperator(BaseOperator):
    @make_kwargs
    def __init__(self, operator: str, *args, **kwargs):
        params = inspect.signature(BaseOperator).parameters
        base_kwargs = {k: kwargs[k] for k in params if k in kwargs}
        super().__init__(**base_kwargs)

        mod_name = kwargs.pop("mod_name")
        op_name = kwargs.pop("op_name")
        self.operator = getattr(importlib.import_module(mod_name), op_name)

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
        task_instance = TaskInstance(
            task=self.task, execution_date=self.start_date, run_id=f"cli__{ts_nodash}"
        )

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
