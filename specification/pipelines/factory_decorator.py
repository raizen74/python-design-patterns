import inspect
from collections.abc import Callable
from functools import partial, wraps
from typing import Any

from main import Runnable

type RunnableDef = Callable[..., Any]
type RunnableFactory[In, Out] = Callable[..., Runnable[In, Out]]
# Allows to pass extra params to runnable function at composition time
def runnable_extra[In, Out](fn: RunnableDef) -> RunnableFactory[In, Out]:
    @wraps(fn)
    def wrapper(**kwargs: Any) -> Runnable[In, Out]:
        # obj is the last positional argument, is the param received in invoke()
        # return Runnable(lambda obj: fn(*args, obj, **kwargs))
        return Runnable(partial(fn, **kwargs)) # enforce partial kwargs

    return wrapper

@runnable_extra
def add(x: int,*, y: int) -> int: # y is the obj param of the wrapper
    print(f"Adding {x} and {y}")
    return x + y

@runnable_extra
def multiply(x: int,*, y: int) -> int:
    print(f"Multiplying {x} and {y}")
    return x * y

runnable_extra_sequence = add(y=5) | multiply(y=2) | add(y=3)  | multiply(y=2) # partial(fn, **kwargs)
print(f"{runnable_extra_sequence!r}")
print(inspect.signature(runnable_extra_sequence.last.func).parameters)
print(f"{runnable_extra_sequence.invoke(10) = }") # pass the first positional param of add -> x

add(y=5).invoke(10)
