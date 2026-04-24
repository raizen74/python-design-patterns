from __future__ import annotations

from dataclasses import dataclass
from functools import partial, wraps
from typing import TYPE_CHECKING, Any, Concatenate, Protocol, TypeGuard, cast

if TYPE_CHECKING:
    from collections.abc import Callable


class Composable[In, Out](Protocol):
    """Protocol for composable pipeline steps."""

    def __call__(self, value: In) -> Out: ...
    def __or__[PipeOut](self, other: Composable[Out, PipeOut]) -> Pipe[In, Out, PipeOut]: ...


class ComposableMixin[In, Out](Composable[In, Out]):
    """Mixin -- adds default __or__ and __and__ implementation to Composable types."""

    def __or__[PipeOut](self, other: Composable[Out, PipeOut]) -> Pipe[In, Out, PipeOut]:
        return Pipe(self, other)

    def __and__[PipeOut](self, other: Composable[Out, PipeOut]) -> Pipe[In, Out, PipeOut]:
        return self | GatedPipe(other)


@dataclass
class Step[In, Out](ComposableMixin[In, Out]):
    """Composable pipeline step that wraps an function."""

    func: Callable[[In], Out]

    def __call__(self, value: In) -> Out:
        res = self.func(value)
        print(f"{self}({value}) = {res}")
        return res

    def __repr__(self) -> str:
        # Handle normal functions and partials
        func_name = getattr(self.func, "__name__", None)
        if func_name is None and hasattr(self.func, "func"):
            func_name = getattr(self.func.func, "__name__", "partial")
        return f"{self.__class__.__name__}(func={func_name or 'unknown'})"


@dataclass
class Pipe[In, Mid, Out](ComposableMixin[In, Out]):
    """Composable pipeline that sequentially composes two steps."""

    first: Composable[In, Mid]
    last: Composable[Mid, Out]

    def __call__(self, value: In) -> Out:
        # Static checkers now verify:
        # In -> Mid -> Out
        intermediate: Mid = self.first(value)
        final: Out = self.last(intermediate)
        print(f"{self}({value}) = {final}")
        return final

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(first={self.first}, last={self.last})"


@dataclass
class GatedPipe[In, Out](ComposableMixin[In, Out]):
    """Composable pipeline that conditionally executes based on a predicate."""

    composable: Composable[In, Out]

    def __call__(self, value: In) -> Out:
        if not bool(value):
            return cast("Out", value)  # short-circuit if falsy
        result = self.composable(value)
        print(f"{self}({value}) = {result}")
        return result

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(composable={self.composable})"


@dataclass
class OldGatedPipe[In, PipeIn, Out](ComposableMixin[In, Out]):
    """Composable pipeline that conditionally executes based on a predicate."""

    predicate: Callable[[In], TypeGuard[PipeIn]]
    pipeline: Composable[PipeIn, Out]

    def __call__(self, value: In) -> Out:
        if self.predicate(value):
            result = self.pipeline(value)
            print(f"{self}({value}) = {result}")
            return result
        return cast("Out", value)

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(predicate={self.predicate.__name__!r}, pipeline={self.pipeline})"


def step[In, Out, **P](fn: Callable[Concatenate[In, P], Out]) -> Callable[..., Step[In, Out]]:
    @wraps(fn)
    def wrapper(**kwargs: Any) -> Step[In, Out]:
        return Step(partial(fn, **kwargs))  # enforce partial kwargs

    return wrapper


@step
def add(x: int, *, y: int) -> int:  # y is the obj param of the wrapper
    # print(f"Adding {x} and {y}")
    return x + y


@step
def multiply(x: int, *, y: int) -> int:
    # print(f"Multiplying {x} and {y}")
    return x * y


@step
def passthrough[T](x: T) -> T:
    print(f"Passthrough {x}")
    return x


def predicate(x: Any) -> TypeGuard[int]:  # narrows the type on a predicate function, x: int if predicate returns True
    print(f"Evaluating predicate for {x}")
    return True


if __name__ == "__main__":
    # add5 = add(y=5)
    # mul10 = multiply(y=10)
    # condition = OldGatedPipe(predicate=predicate, pipeline=(add5 | mul10))
    # print(f"{condition!r}")
    # pipeline = add5 | condition
    # print(f"{pipeline!r}")
    # result = asyncio.run(pipeline(5))
    # print(f"Pipeline result: {result}")

    add5 = add(y=5)
    mul0 = multiply(y=0)
    mul10 = multiply(y=10)
    pipeline = add5 | mul10 & mul10 | mul10
    print(f"{pipeline!r}")
    result = pipeline(0)
    print(f"Pipeline result: {result}")
