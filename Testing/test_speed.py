import time
from typing import Any, Callable


def calculate_time(fn: Callable[[Any], Any]):
    in_prog = False

    def inner(*args, **kwargs):
        nonlocal in_prog
        if not in_prog:
            print("Starting running timer")
            in_prog = True
            start = time.time()
            result = fn(*args, **kwargs)

            end = time.time()

            print(f'Running {fn.__name__} with', end='')
            if len(str(**kwargs)) > 100 or len(str(*args)) > 100:
                print(f'took {end - start} time')
            else:

                print(f'{args, kwargs} took {end - start} time')
            in_prog = False

            return result
        else:
            return fn(*args, **kwargs)
    return inner
