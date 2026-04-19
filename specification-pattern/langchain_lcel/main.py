from collections.abc import Callable
from dataclasses import dataclass
from functools import partial, wraps
from typing import Any


# The [In, Out] syntax automatically creates TypeVars and marks the class as Generic
@dataclass
class Runnable[In, Out]:
    func: Callable[[In], Out]

    # We introduce 'NextOut' scoped only to this method
    def __or__[NextOut](self, other: Runnable[Out, NextOut]) -> RunnableSequence[In, Out, NextOut]:
        print(f"{self}.__or__({other})")
        return RunnableSequence(self, other)

    def __ror__[PrevIn](self, other: Callable[[PrevIn], In]) -> RunnableSequence[PrevIn, In, Out]:
        # Wrap the raw callable into a Runnable to maintain the chain
        return RunnableSequence(Runnable(other), self)

    def __repr__(self) -> str:
    # Handle normal functions and partials
        func_name = getattr(self.func, "__name__", None)

        if func_name is None and hasattr(self.func, "func"):
            # It's likely a functools.partial object
            func_name = getattr(self.func.func, "__name__", "partial")

        return f"{self.__class__.__name__}(func={func_name or 'unknown'})"

    def bind(self, **kwargs) -> None:
        """Mutates the func attribute in-place with partial."""
        self.func = partial(self.func, **kwargs)

    def invoke(self, value) -> Out:
        args = (value, ) if value is not None else ()
        res = self.func(*args)
        print(f"{self}.invoke({value}) = {res}")
        return res


# This sequence tracks the start (In), the handshake type (Mid), and the end (Out)
class RunnableSequence[In, Mid, Out](Runnable[In, Out]):
    def __init__(self, first: Runnable[In, Mid], last: Runnable[Mid, Out]):
        self.first = first
        self.last = last

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(first={self.first!r}, last={self.last!r})"

    def invoke(self, value: In | None = None) -> Out:
        print(f"{self}.invoke({value})")
        # Static checkers now verify:
        # In -> Mid -> Out
        intermediate: Mid = self.first.invoke(value)
        print(f"{self}.invoke({value}) = {intermediate}")
        final: Out = self.last.invoke(intermediate)
        print(f"{self}.invoke({value}) = {final}")
        return final


# ------------------------------------------------------------
# Decorators
# ------------------------------------------------------------

type RunnableFn[In, Out] = Callable[[In], Out]
type RunnableFactory[In, Out] = Callable[..., Runnable[In, Out]]

def runnable[In, Out](fn: RunnableFn[In, Out]) -> Runnable[In, Out]:
    @wraps(fn)
    def wrapper(*args: Any, **kwargs: Any) -> Out:
        return fn(*args, **kwargs) # forwards partial kwargs to fn
    # def wrapper(obj: In) -> Out: # Initial implementation
    #     return fn(obj)
    return Runnable(wrapper)

@runnable
def add_five(x: int) -> int:
    return x + 5


@runnable
def multiply_by_two(x: int) -> int:
    return x * 2


@runnable
def multiply_three_vars(x: int, y: int, z: int) -> int:
    return x * y * z

# Let's define some "steps"
# Composition: No math is done yet!
# This creates a RunnableSequence(add_five, multiply_by_two)
runnable_sequence: RunnableSequence = add_five | multiply_by_two
print(f"{runnable_sequence.invoke(10) = }")
print("-----------------------------------------------------------------------")

# Partially initialize dependencies
add_five.bind(x=5) # kwarg passed to wrapper, wrapper forwards it to fn
multiply_three_vars.bind(y=3, z=4)
runnable_sequence_bound: RunnableSequence = add_five | multiply_by_two | multiply_three_vars
print(f"{runnable_sequence_bound!r}")
print(f"{runnable_sequence_bound.invoke() = }")
print("-----------------------------------------------------------------------")

type RunnableDef = Callable[..., Any]

# Allows to pass extra params to runnable function at composition time
def runnable_extra[In, Out](fn: RunnableDef) -> RunnableFactory[In, Out]:
    @wraps(fn)
    def wrapper(*args: Any, **kwargs: Any) -> Runnable[In, Out]:
        # obj is the last positional argument, is the param received in invoke()
        return Runnable(lambda obj: fn(*args, obj, **kwargs))

    return wrapper

@runnable_extra
def add(x: int, y: int) -> int: # y is the obj param of the wrapper
    print(f"Adding {x} and {y}")
    return x + y

@runnable_extra
def multiply(x: int, y: int) -> int:
    print(f"Multiplying {x} and {y}")
    return x * y

runnable_extra_sequence = add(5) | multiply(2) # 5 and 2 are the x params, the *args

print(f"{runnable_extra_sequence.invoke(10) = }") # 10 is the y param (obj of the decorator)
print("-----------------------------------------------------------------------")

# ------------------------------------------------------------
# CONDITIONAL BRANCHING
# ------------------------------------------------------------

class RunnableBranch[In, Out](Runnable[In, Out]):
    def __init__(
        self,
        condition: Callable[[In], bool],
        on_true: Runnable[In, Out],
        on_false: Runnable[In, Out],
    ):
        self.condition = condition
        self.on_true = on_true
        self.on_false = on_false

    def __repr__(self):
        return f"{self.__class__.__name__}(condition={self.condition!r}, on_true={self.on_true!r}, on_false={self.on_false!r})"

    def invoke(self, value: In) -> Out:
        # The routing logic happens here at runtime
        if self.condition(value):
            return self.on_true.invoke(value)
        return self.on_false.invoke(value)

def condition(value: int) -> bool:
    print(f"Checking condition value > 5 for value: {value}")
    return value > 10

@runnable
def add_five(x: int) -> int:
    return x + 5

@runnable
def multiply_by_two(x: int) -> int:
    return x * 2

# Routing logic: If result > 50, continue processing. Else, return immediately.
conditional = RunnableBranch(
    condition=lambda x: x > 10,
    on_true=add_five | multiply_by_two | multiply_by_two | add_five,
    on_false=Runnable(lambda x: x), # Returns the result of step1 immediately
)

# The Pipeline
conditional_pipeline: RunnableSequence[int, int, int] = add_five | conditional

print(f"{conditional_pipeline.invoke(20) = }")
print("-----------------------------------------------------------------------")


# ------------------------------------------------------------
# Executing multiple functions
# ------------------------------------------------------------
# # Input flows into both branches simultaneously
class RunnableParallel(Runnable):
    def __init__(self, steps_dict):
        self.steps_dict = steps_dict

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(steps_dict={self.steps_dict!r})"

    def invoke(self, value):
        # Runs all branches and returns a combined dictionary
        return {key: step.invoke(value) for key, step in self.steps_dict.items()}

branch_chain = RunnableParallel(
    {
        "plus_five": add_five,
        "times_two": multiply_by_two,
    },
)

# The "Pipeline" definition
parallel_pipeline: RunnableSequence = branch_chain | Runnable(lambda x: x["plus_five"] + x["times_two"])

# DEFERRED EXECUTION:
# This is the moment the logic actually fires.
print(f"{parallel_pipeline.invoke(10) = }")
print("-----------------------------------------------------------------------")
