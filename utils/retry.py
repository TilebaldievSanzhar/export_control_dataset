"""Retry logic for API calls."""

import asyncio
import functools
import time
from typing import Callable, Type, Union


def retry(
    max_attempts: int = 3,
    delay: float = 5,
    backoff: float = 2,
    exceptions: tuple[Type[Exception], ...] = (Exception,),
) -> Callable:
    """
    Retry decorator for synchronous functions.

    Args:
        max_attempts: Maximum number of retry attempts
        delay: Initial delay between retries in seconds
        backoff: Multiplier for delay after each retry
        exceptions: Tuple of exception types to catch

    Returns:
        Decorated function
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            current_delay = delay
            last_exception = None

            for attempt in range(max_attempts):
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    last_exception = e
                    if attempt < max_attempts - 1:
                        time.sleep(current_delay)
                        current_delay *= backoff

            raise last_exception

        return wrapper
    return decorator


def retry_async(
    max_attempts: int = 3,
    delay: float = 5,
    backoff: float = 2,
    exceptions: tuple[Type[Exception], ...] = (Exception,),
) -> Callable:
    """
    Retry decorator for async functions.

    Args:
        max_attempts: Maximum number of retry attempts
        delay: Initial delay between retries in seconds
        backoff: Multiplier for delay after each retry
        exceptions: Tuple of exception types to catch

    Returns:
        Decorated function
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            current_delay = delay
            last_exception = None

            for attempt in range(max_attempts):
                try:
                    return await func(*args, **kwargs)
                except exceptions as e:
                    last_exception = e
                    if attempt < max_attempts - 1:
                        await asyncio.sleep(current_delay)
                        current_delay *= backoff

            raise last_exception

        return wrapper
    return decorator


class RetryContext:
    """Context manager for retrying operations."""

    def __init__(
        self,
        max_attempts: int = 3,
        delay: float = 5,
        backoff: float = 2,
        exceptions: tuple[Type[Exception], ...] = (Exception,),
    ):
        self.max_attempts = max_attempts
        self.delay = delay
        self.backoff = backoff
        self.exceptions = exceptions
        self.attempt = 0
        self.current_delay = delay

    def __iter__(self):
        return self

    def __next__(self) -> int:
        if self.attempt >= self.max_attempts:
            raise StopIteration

        if self.attempt > 0:
            time.sleep(self.current_delay)
            self.current_delay *= self.backoff

        self.attempt += 1
        return self.attempt

    def reset(self) -> None:
        """Reset retry counter."""
        self.attempt = 0
        self.current_delay = self.delay
