from __future__ import annotations

from collections import Counter
from functools import wraps, partial
from dataclasses import dataclass

import streamlit as st
from types import FunctionType
from typing import Any
import sys
import hashlib
import inspect
from threading import Semaphore, Lock, Condition


@dataclass
class FuncConcurrencyInfo:
    semaphore: Semaphore
    condition: Condition


SEMAPHORES_LOCK = Lock()
CONCURRENCY_MAP: dict[str, FuncConcurrencyInfo] = {}


COUNTERS = Counter()


def _make_function_key(func: FunctionType, max_concurrency: int) -> str:
    """Create the unique key for a function's cache.
    A function's key is stable across reruns of the app, and changes when
    the function's source code changes.
    """

    hashlib_kwargs: dict[str, Any] = (
        {"usedforsecurity": False} if sys.version_info >= (3, 9) else {}
    )
    func_hasher = hashlib.new("md5", **hashlib_kwargs)

    func_hasher.update(func.__module__.encode("utf-8"))
    func_hasher.update(func.__qualname__.encode("utf-8"))

    try:
        source_code = inspect.getsource(func).encode("utf-8")
    except OSError:
        source_code = func.__code__.co_code

    func_hasher.update(source_code)
    func_hasher.update(max_concurrency.to_bytes(4, byteorder="big"))

    return func_hasher.hexdigest()


def concurrency_limiter(func=None, max_concurrency=1, show_spinner: bool = True):

    if func is None:
        return partial(
            concurrency_limiter,
            max_concurrency=max_concurrency,
            show_spinner=show_spinner,
        )

    function_key = _make_function_key(func, max_concurrency)

    with SEMAPHORES_LOCK:
        if function_key not in CONCURRENCY_MAP:
            CONCURRENCY_MAP[function_key] = FuncConcurrencyInfo(
                semaphore=Semaphore(max_concurrency),
                condition=Condition(),
            )

    @wraps(func)
    def wrapper(*args, **kwargs):
        func_info = CONCURRENCY_MAP[function_key]
        acquired = False

        COUNTERS.update({function_key: 1})

        try:
            with func_info.condition:
                while not (acquired := func_info.semaphore.acquire(blocking=False)):
                    if show_spinner:
                        with st.spinner(
                            f"""Function {func.__name__} has approximately
                            {COUNTERS[function_key] - max_concurrency } instances
                            waiting...""",
                            _cache=True,
                        ):

                            func_info.condition.wait()
                    else:
                        func_info.condition.wait()

            return func(*args, **kwargs)
        finally:
            COUNTERS.update({function_key: -1})
            with func_info.condition:
                if acquired:
                    func_info.semaphore.release()
                func_info.condition.notify_all()

    return wrapper
