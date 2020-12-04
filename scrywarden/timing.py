"""Contains helper utilities for timing based functionality."""

import functools
import logging
import random
import time
import typing as t

logger = logging.getLogger(__name__)


class ExponentialBackoff:
    """Manages a crude implementation of exponential backoff.

    This implementation keeps track of the number of previous attempts at an
    activity and returns an exponentially increasing timeout value after
    each iteration. After a number of attempts, the exponential backoff will
    taper and start to increase at the rate of a given dividend value.

    Parameters
    ----------
    after: int
        The number of attempts it will take until the exponential backoff
        is canceled.
    dividend: float
        The dividend used to determine the next incremental value for backoff.
        This is calculated by dividend / number of attempts past the given
        threshold.
    initialize: bool
        If the initial timeout value of 0 should already be consumed. Usually
        the first call to `next()` will return 0, but if the backoff is being
        increased elsewhere, such as if the next backoff time needs to be
        printed to logs, then the initial timeout value will be the timeout
        if the backoff was already iterated.

    Examples
    --------
    >>> import threading
    >>> from scrywarden.timing import ExponentialBackoff
    >>> backoff = ExponentialBackoff()
    >>> event = threading.Event()
    >>> while event.wait(backoff.next()):
    >>>     print(f"Waited {backoff.timeout:.2f} seconds")
    """
    def __init__(
        self,
        after: int = 2,
        dividend: float = 1.,
        initialize: bool = False,
    ):
        self._after: int = after
        self._timeout: float = 0.0
        self._attempts: int = 0
        self._dividend: float = dividend
        self._additional: float = 0.0
        self._next: t.Callable[[], float] = (
            self._calculate_timeout if initialize else self._return_initial
        )

    @property
    def timeout(self) -> float:
        """Returns the current timeout value."""
        return self._timeout

    @property
    def attempts(self) -> int:
        """Returns the current number of attempts."""
        return self._attempts

    def reset(self, initialize: bool = False) -> None:
        """Reset the backoff.

        Parameters
        ----------
        initialize: bool
            If the initial timeout value of 0 should already be consumed.
        """
        self._timeout = 0.0
        self._attempts = 0
        self._additional = 0.0
        self._next = (
            self._calculate_timeout if initialize else self._return_initial
        )

    def next(self) -> float:
        """Returns the next timeout value.

        Typically the first returned value is 0 unless `initialize` is set
        to True during object initialization.

        Returns
        -------
        float
            Next timeout value.
        """
        self._timeout = self._next()
        return self._timeout

    def _increment_attempts(self) -> int:
        attempts = self._attempts
        self._attempts += 1
        return attempts

    def _return_initial(self) -> float:
        self._next = self._calculate_timeout
        return self._timeout

    def _calculate_timeout(self) -> float:
        attempts = self._increment_attempts()
        if attempts >= self._after:
            self._next = self._calculate_past_threshold
        return (attempts ** 2) + (random.randint(0, 1000) / 1000)

    def _calculate_past_threshold(self) -> float:
        attempts = self._increment_attempts()
        self._additional += self._dividend / (attempts - self._after)
        return (
            (self._after ** 2) + self._additional
            + (random.randint(0, 1000) / 1000)
        )


class benchmark:
    """Decorator + context manager that keeps track of a process time.

    Parameters
    ----------
    message: str
        Message to log after the benchmark is finished. Messages should have
        a floating point string identifier like `%.2f` in it otherwise an
        error will be thrown during printing.
    logger: logging.Logger
        Logger instance to log the given message to. Defaults to the one in
        the `scrywarden.timing` module.
    level: int
        Logging level to print the benchmark message at.

    Examples
    --------
    >>> with backoff() as elapsed:
    >>>     print(f"{elapsed():.2f} seconds have elapsed")

    >>> with backoff("Section completed in %.2f seconds") as elapsed:
    >>>     print("Hello!")

    >>> @backoff("Printing took %.2f seconds")
    >>> def print_hello():
    >>>     print("Hello!")
    """
    def __init__(
        self,
        message: str = '',
        logger: logging.Logger = logger,
        level: int = logging.INFO,
    ):
        self._message: t.Optional[str] = message
        self._logger: logging.Logger = logger
        self._level: int = level
        self._start: t.Optional[float] = None

    def __call__(self, func: t.Callable) -> t.Callable:
        """Decorates a function call.

        Parameters
        ----------
        func: Callable
            Callable to wrap with the benchmark logging.

        Returns
        -------
        Callable
            Wrapped callable.
        """
        message = (
            self._message if self._message
            else f"{func.__name__} took %.2f seconds"
        )

        @functools.wraps(func)
        def callback(*args, **kwargs):
            start = time.time()
            result = func(*args, **kwargs)
            self._logger.log(
                self._level, message, time.time() - start,
            )
            return result

        return callback

    def __enter__(self) -> t.Callable[[], float]:
        """Starts the context manager.

        Returns
        -------
        Callable[[], float]
            Callable that returns the number of seconds elapsed when called.
        """
        self._start = time.time()
        return lambda: time.time() - self._start

    def __exit__(
        self,
        cls: t.Optional[t.Type[Exception]],
        value: t.Optional[Exception],
        traceback: t.Optional,
    ) -> bool:
        """Ends the context management.

        Parameters
        ----------
        cls: Optional[Type[Exception]]
            Error type thrown.
        value: Optional[Exception]
            Error value thrown.
        traceback
            Traceback of the error.

        Returns
        -------
        bool
            True if the error was handled, false otherwise.
        """
        if cls or not self._message:
            return False
        logger.log(self._level, self._message, time.time() - self._start)
        return False
