import importlib
from typing import Any, Callable, Dict, List, Tuple


def _multi_tuple_to_dict(ctx, param, value: List[Tuple[str, Any]]) -> Dict[str, Any]:
    if value is not None:
        return {k: v for k, v in value}
    return {}


def _remove_unused_kwargs(kwargs) -> Dict[str, Any]:
    return {k: v for k, v in kwargs.items() if v is not None}


def _str_to_callable(ctx, param, value: str) -> Callable:
    if value is not None:
        mod_name, func_name = value.split(":")
        mod = importlib.import_module(mod_name)
        func = getattr(mod, func_name)
        if not isinstance(func, Callable):
            raise TypeError(f"Provided attribute must be callable: {value}")
        return func
