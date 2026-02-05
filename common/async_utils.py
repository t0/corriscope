# Standard Python packages
import asyncio


#PyPi packages

import nest_asyncio


def run_async(awaitable):
    """
    Run the current loop until the awaitable is resolved.

    Is used to run an async function from a sync function, assuming there is a valid current event loop .

    We use nest_asyncio to allow run_until_complete() call to operate even if the current loop is already running. Native asyncio does not allow that.
    """
    nest_asyncio.apply()  # make sure we can run in an already running loop
    return asyncio.run(awaitable)


def async_to_sync(fn):
    """
    Wraps an async coroutine function into a function that can be called synchronously (without the await statement).

    This assumes there is a event loop.
    """
    def sync_fn(*args, **kwargs):
        return run_async(fn(*args, **kwargs))
    return sync_fn

