import asyncio
import os
import socket
import inspect
import collections

HOST_KEY = 'host'
CANCELLED_KEY = 'cancelled'
RESULT_KEY = 'result'
EXCEPTION_KEY = 'exception'
PENDING_KEY = 'pending'

__all__ = []

def get_host_info():
    return {'hostname': socket.gethostname(), 'pid': os.getpid()}

def ensure_coroutine(coro_or_fn):
    if asyncio.iscoroutinefunction(coro_or_fn):
        return coro_or_fn
    if callable(coro_or_fn):
        if inspect.isclass(coro_or_fn):
            coro_or_fn = coro_or_fn.__call__
        return asyncio.coroutine(coro_or_fn)

def cancelled_response(msg=None):
    return {CANCELLED_KEY: msg}

def result_response(result):
    return {RESULT_KEY: result}

def pending_response(msg=None):
    return {PENDING_KEY: msg}

def exception_response(exception, trace=None):
    """
    Create an exception response dictionary
    :param exception: The exception to encode
    :type exception: :class:`Exception`
    :param trace: Optional traceback
    :return: A response dictionary
    :rtype: dict
    """
    msg = str(exception)
    if trace is not None:
        msg += "\n{}".format(trace)
    return {EXCEPTION_KEY: msg}

def response_to_future(response, future=None):
    """
    Take aresponse message and set the appropriate value on the given future
    :param response:
    :param future:
    :return:
    """
    if not isinstance(response, collections.Mapping):
        raise TypeError("Response must be a mapping")

    if future is None:
        future = asyncio.Future()

    if CANCELLED_KEY in response:
        future.cancel()
    elif EXCEPTION_KEY in response:
        future.set_exception(kiwipy.RemoteException(response[EXCEPTION_KEY]))
    elif RESULT_KEY in response:
        future.set_result(response[RESULT_KEY])
    elif PENDING_KEY in response:
        future.set_result(asyncio.Future())
    else:
        raise ValueError("Unknown response type '{}'".format(response))

    return future
