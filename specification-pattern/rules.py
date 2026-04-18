from collections.abc import Callable
from functools import wraps
from typing import Any

# ------------------------------------------------------------
# Generic Types
# ------------------------------------------------------------

type PredicateFn[T] = Callable[[T], bool]
type RuleDef = Callable[..., bool]
type PredicateFactory[T] = Callable[..., Predicate[T]]


# ------------------------------------------------------------
# Global Rule Registry
# ------------------------------------------------------------

RULES: dict[str, PredicateFactory[Any]] = {}


# ------------------------------------------------------------
# Predicate
# ------------------------------------------------------------


class Predicate[T]:
    """
    A composable predicate that supports &, |, and ~ operators.
    Wraps a function (T -> bool).
    """

    def __init__(self, fn: PredicateFn[T]):
        self.fn = fn

    def __call__(self, obj: T) -> bool:
        return self.fn(obj)

    def __and__(self, other: Predicate[T]) -> Predicate[T]:
        return Predicate(lambda x: self(x) and other(x))

    def __or__(self, other: Predicate[T]) -> Predicate[T]:
        return Predicate(lambda x: self(x) or other(x))

    def __invert__(self) -> Predicate[T]:
        return Predicate(lambda x: not self(x))


# ------------------------------------------------------------
# Decorators
# ------------------------------------------------------------

# Does not allow to pass extra params to predicate functions
def predicate[T](fn: PredicateFn[T]) -> Predicate[T]:
    @wraps(fn)
    def wrapper(obj: T) -> bool:
        return fn(obj)

    return Predicate(wrapper)


# Allows to pass extra params to PredicateFn
def rule[T](fn: RuleDef) -> PredicateFactory[Any]:
    @wraps(fn)
    def wrapper(*args: Any, **kwargs: Any) -> Predicate[T]:
        return Predicate(lambda obj: fn(*args, obj, **kwargs))

    RULES[fn.__name__] = wrapper
    return wrapper
