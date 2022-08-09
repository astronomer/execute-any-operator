from typing import Any, Dict, List, Tuple


def _multi_tuple_to_dict(ctx, param, value: List[Tuple[str, Any]]) -> Dict[str, Any]:
    if value is not None:
        return {k: v for k, v in value}
    return {}


def _remove_unused_kwargs(kwargs) -> Dict[str, Any]:
    return {k: v for k, v in kwargs.items() if v is not None}
