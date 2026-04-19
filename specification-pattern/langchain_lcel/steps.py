from main import RunnableSequence, runnable


@runnable
def add_five(x: int) -> int:
    return x + 5

@runnable
def multiply_by_two(x: int) -> int:
    return x * 2

@runnable
def multiply_three_vars(x: int, y: int, z: int) -> int:
    return x * y * z

add_five.bind(x=5) # kwarg passed to wrapper, wrapper forwards it to fn
multiply_three_vars.bind(y=3, z=4)

runnable_sequence_bound: RunnableSequence = add_five | multiply_by_two | multiply_three_vars
print(f"Pipeline = {runnable_sequence_bound!r}")

print(f"{runnable_sequence_bound.invoke() = }")
