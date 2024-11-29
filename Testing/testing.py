from dataclasses import dataclass
from typing import Any, Callable, List
import retrieve_query


@dataclass
class FunctionAndParams:
    fn: Callable[[Any], Any]
    params: list[Any]


functions_speed_testing: list[FunctionAndParams] = FunctionAndParams()
