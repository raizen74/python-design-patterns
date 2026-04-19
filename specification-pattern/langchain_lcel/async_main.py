import asyncio
from typing import Callable, Any, Awaitable

# Decorator for async Runnable 
def async_runnable(func: Callable[..., Awaitable[Any]]) -> Callable[..., Awaitable[Any]]:
    async def wrapper(*args, **kwargs) -> Any:
        print(f"Executing {func.__name__}...")
        return await func(*args, **kwargs)
    return wrapper

class AsyncRunnable:
    @async_runnable
    async def run(self):
        # Simulating an async operation
        await asyncio.sleep(2)
        print('AsyncRunnable finished executing!')

class Pipeline:
    def __init__(self, *runnables: AsyncRunnable):
        self.runnables = runnables

    async def execute(self):
        for runnable in self.runnables:
            await runnable.run()

# Example async functions
class ExampleAsyncRunnable1(AsyncRunnable):
    @async_runnable
    async def run(self):
        await asyncio.sleep(1)
        print('ExampleAsyncRunnable1 executed!')

class ExampleAsyncRunnable2(AsyncRunnable):
    @async_runnable
    async def run(self):
        await asyncio.sleep(1)
        print('ExampleAsyncRunnable2 executed!')

# Pipeline execution example
if __name__ == '__main__':
    pipeline = Pipeline(ExampleAsyncRunnable1(), ExampleAsyncRunnable2())
    asyncio.run(pipeline.execute())