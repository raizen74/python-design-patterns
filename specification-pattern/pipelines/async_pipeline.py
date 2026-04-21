from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from functools import partial, wraps
from typing import TYPE_CHECKING, Any, Concatenate, Protocol, TypeGuard, cast

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable


class Composable[In, Out](Protocol):
    """Protocol for composable pipeline steps."""

    async def __call__(self, value: In) -> Out: ...
    def __or__[PipeOut](self, other: Composable[Out, PipeOut]) -> Pipe[In, Out, PipeOut]: ...


@dataclass
class Step[In, Out](Composable[In, Out]):
    """Composable pipeline step that wraps an async function."""

    func: Callable[[In], Awaitable[Out]]

    async def __call__(self, value: In) -> Out:
        res = await self.func(value)
        print(f"{self}({value}) = {res}")
        return res

    def __or__[PipeOut](self, other: Composable[Out, PipeOut]) -> Pipe[In, Out, PipeOut]:
        return Pipe(self, other)

    def __repr__(self) -> str:
        # Handle normal functions and partials
        func_name = getattr(self.func, "__name__", None)
        if func_name is None and hasattr(self.func, "func"):
            func_name = getattr(self.func.func, "__name__", "partial")
        return f"{self.__class__.__name__}(func={func_name or 'unknown'})"

@dataclass
class Pipe[In, Mid, Out](Composable[In, Out]):
    """Composable pipeline that sequentially composes two steps."""

    first: Composable[In, Mid]
    last: Composable[Mid, Out]

    async def __call__(self, value: In) -> Out:
        print(f"{self}({value})")
        # Static checkers now verify:
        # In -> Mid -> Out
        intermediate: Mid = await self.first(value)
        final: Out = await self.last(intermediate)
        return final

    def __or__[PipeOut](self, other: Composable[Out, PipeOut]) -> Pipe[In, Out, PipeOut]:
        return Pipe(self, other)

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(first={self.first}, last={self.last})"



@dataclass
class GatedPipe[In, PipeIn, Out](Composable[In, Out]):
    """Composable pipeline that conditionally executes based on a predicate."""

    predicate: Callable[[In], TypeGuard[PipeIn]]
    pipeline: Composable[PipeIn, Out]

    async def __call__(self, value: In) -> Out:
        print(f"{value = } {self.predicate(value) = }")
        if self.predicate(value):
            return await self.pipeline(value)
        return cast("Out", value)

    def __or__[PipeOut](self, other: Composable[Out, PipeOut]) -> Pipe[In, Out, PipeOut]:
        return Pipe(self, other)

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}(predicate={self.predicate.__name__!r}, pipeline={self.pipeline})"
        )


def step[In, Out, **P](fn: Callable[Concatenate[In, P], Awaitable[Out]]) -> Callable[..., Step[In, Out]]:
    @wraps(fn)
    def wrapper(**kwargs: Any) -> Step[In, Out]:
        return Step(partial(fn, **kwargs))  # enforce partial kwargs

    return wrapper


@step
async def add(x: int, *, y: int) -> int:  # y is the obj param of the wrapper
    await asyncio.sleep(0.1)  # Simulate async work
    # print(f"Adding {x} and {y}")
    return x + y


@step
async def multiply(x: int, *, y: int) -> int:
    await asyncio.sleep(0.1)  # Simulate async work
    # print(f"Multiplying {x} and {y}")
    return x * y


@step
async def passthrough[T](x: T) -> T:
    await asyncio.sleep(0.1)  # Simulate async work
    print(f"Passthrough {x}")
    return x


def predicate(x: Any) -> TypeGuard[int]: # narrows the type on a predicate function, x: int if predicate returns True
    print(f"Evaluating predicate for {x}")
    return True


if __name__ == "__main__":
    add5 = add(y=5)
    mul10 = multiply(y=10)
    condition = GatedPipe(predicate=predicate, pipeline=(add5 | mul10))
    print(f"{condition!r}")
    pipeline = add5 | condition
    print(f"{pipeline!r}")

    result = asyncio.run(pipeline(5))
    print(f"Pipeline result: {result}")
