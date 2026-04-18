from collections.abc import Callable
from dataclasses import dataclass
from functools import wraps
from typing import Any


# The [In, Out] syntax automatically creates TypeVars and marks the class as Generic
@dataclass
class Runnable[In, Out]:
    func: Callable[[In], Out]

    # We introduce 'NextOut' scoped only to this method
    def __or__[NextOut](self, other: Runnable[Out, NextOut]) -> RunnableSequence[In, Out, NextOut]:
        return RunnableSequence(self, other)

    def __ror__[PrevIn](self, other: Callable[[PrevIn], In]) -> RunnableSequence[PrevIn, In, Out]:
        # Wrap the raw callable into a Runnable to maintain the chain
        return RunnableSequence(Runnable(other), self)

    def invoke(self, value: In) -> Out:
        print(f"Calling {self.func.__name__}")
        return self.func(value)


# This sequence tracks the start (In), the handshake type (Mid), and the end (Out)
class RunnableSequence[In, Mid, Out](Runnable[In, Out]):
    def __init__(self, first: Runnable[In, Mid], last: Runnable[Mid, Out]):
        self.first = first
        self.last = last

    def invoke(self, value: In) -> Out:
        print("Calling RunnableSequence")
        # Static checkers now verify:
        # In -> Mid -> Out
        intermediate: Mid = self.first.invoke(value)
        print(f"Intermediate value: {intermediate}")
        return self.last.invoke(intermediate)


# ------------------------------------------------------------
# Decorators
# ------------------------------------------------------------

type RunnableFn[In, Out] = Callable[[In], Out]
type RunnableDef = Callable[..., Any]
type RunnableFactory[In, Out] = Callable[..., Runnable[In, Out]]

def runnable[In, Out](fn: RunnableFn[In, Out]) -> Runnable[In, Out]:
    @wraps(fn)
    def wrapper(obj: In) -> Out:
        return fn(obj)

    return Runnable(wrapper)


# Allows to pass extra params to runnable function
def runnable_extra[In, Out](fn: RunnableDef) -> RunnableFactory[In, Out]:
    @wraps(fn)
    def wrapper(*args: Any, **kwargs: Any) -> Runnable[In, Out]:
        return Runnable(lambda obj: fn(*args, obj, **kwargs)) # obj is the param received in invoke()

    return wrapper

@runnable
def add_five(x: int) -> int:
    return x + 5


@runnable
def multiply_by_two(x: int) -> int:
    return x * 2

@runnable_extra
def add(x: int, y: int) -> int: # y is the obj parameter of the wrapper
    print(f"Adding {x} and {y}")
    return x + y

@runnable_extra
def multiply(x: int, y: int) -> int:
    print(f"Multiplying {x} and {y}")
    return x * y


# Let's define some "steps"
# Composition: No math is done yet!
# This creates a RunnableSequence(add_five, multiply_by_two)
runnable_sequence: RunnableSequence = add_five | multiply_by_two

# print(f"{vars(runnable_sequence) = }")
# print(f"{type(runnable_sequence) = }")

print(f"{runnable_sequence.invoke(10) = }")

runnable_extra_sequence = add(5) | multiply(2) # 5 and 2 are the x params, the *args

print(f"{runnable_extra_sequence.invoke(10) = }") # 10 is the y param (obj of the decorator)


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

    def invoke(self, value: In) -> Out:
        # The routing logic happens here at runtime
        if self.condition(value):
            return self.on_true.invoke(value)
        return self.on_false.invoke(value)

# A simple Passthrough that returns the input as-is
class Passthrough[T](Runnable[T, T]):
    def __init__(self):
        super().__init__(lambda x: x)

def condition(value: int) -> bool:
    print(f"Checking condition value > 5 for value: {value}")
    return value > 10


# Routing logic: If result > 50, continue processing. Else, return immediately.
conditional = RunnableBranch(
    condition=condition,
    on_true=add_five | multiply_by_two,
    on_false=Passthrough(), # Returns the result of step1 immediately
)

# The Pipeline
conditional_pipeline: RunnableSequence[int, int, int] = add_five | conditional

print(f"{conditional_pipeline.invoke(10) = }")

# ------------------------------------------------------------
# Executing multiple functions
# ------------------------------------------------------------
# # Input flows into both branches simultaneously
class RunnableParallel(Runnable):
    def __init__(self, steps_dict):
        self.steps_dict = steps_dict

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
