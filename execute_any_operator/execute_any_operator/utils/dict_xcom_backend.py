from typing import Any, Iterable, Optional, Union

from airflow.models.xcom import BaseXCom
from airflow.utils.helpers import is_container

XComData = {}


def _update(d, u):
    for k, v in u.items():
        if isinstance(v, dict):
            d[k] = _update(d.get(k, {}), v)
        else:
            d[k] = v
    return d


class DictXComBackend(BaseXCom):

    @classmethod
    def set(cls, key, value, task_id, dag_id, **kwargs):
        """
        Store an XCom value.

        :return: None
        """
        value = BaseXCom.serialize_value(value)

        _update(XComData, {dag_id: {task_id: {key: value}}})

    @classmethod
    def get_one(cls, key: Optional[str] = None, task_id: Optional[Union[str, Iterable[str]]] = None, dag_id: Optional[Union[str, Iterable[str]]] = None, **kwargs) -> Optional[Any]:
        results = cls.get_many(key=key, task_ids=task_id, dag_ids=dag_id)
        if results:
            return results[0]
        return None

    @classmethod
    def get_many(cls, key: Optional[str] = None, task_ids: Optional[Union[str, Iterable[str]]] = None, dag_ids: Optional[Union[str, Iterable[str]]] = None, **kwargs) -> Iterable[Any]:
        if not is_container(task_ids):
            task_ids = [task_ids]

        if not is_container(dag_ids):
            dag_ids = [dag_ids]

        results = tuple()
        for dag_id in dag_ids:
            for task_id in task_ids:
                try:
                    value = XComData[dag_id][task_id][key]
                    results = results + (BaseXCom.deserialize_value(value),)
                except KeyError:
                    pass
        return results

    @classmethod
    def delete(cls, xcoms, **kwargs):
        if not is_container(xcoms):
            xcoms = [xcoms]

        for xcom in xcoms:
            try:
                del XComData[xcom.dag_id][xcom.task_id][xcom.key]
            except KeyError:
                pass

    @classmethod
    def clear(cls, dag_id: str = None, task_id: str = None, **kwargs) -> None:
        try:
            del XComData[dag_id][task_id]
        except KeyError:
            pass
