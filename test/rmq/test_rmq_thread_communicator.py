# -*- coding: utf-8 -*-
# pylint: disable=invalid-name, redefined-outer-name
import concurrent.futures
import pathlib
import unittest

import pytest
import shortuuid

import kiwipy
from kiwipy import rmq

from ..utils import CommunicatorTester
from . import utils

WAIT_TIMEOUT = 5.


@pytest.fixture
def thread_communicator():
    message_exchange = f'{__file__}.{shortuuid.uuid()}'
    task_exchange = f'{__file__}.{shortuuid.uuid()}'
    task_queue = f'{__file__}.{shortuuid.uuid()}'

    communicator = rmq.RmqThreadCommunicator.connect(
        connection_params={'url': 'amqp://guest:guest@localhost:5672/'},
        message_exchange=message_exchange,
        task_exchange=task_exchange,
        task_queue=task_queue,
        testing_mode=True
    )

    yield communicator

    communicator.close()


@pytest.fixture
def thread_task_queue(thread_communicator: rmq.RmqThreadCommunicator):
    task_queue_name = f'{__file__}.{shortuuid.uuid()}'

    task_queue = thread_communicator.task_queue(task_queue_name)

    yield task_queue


class TestRmqThreadCommunicator(CommunicatorTester, unittest.TestCase):
    """Use the standard tests cases to check the RMQ thread communicator"""

    def create_communicator(self):
        message_exchange = f'{self.__class__.__name__}.message_exchange.{shortuuid.uuid()}'
        task_exchange = f'{self.__class__.__name__}.task_exchange.{shortuuid.uuid()}'
        task_queue = f'{self.__class__.__name__}.task_queue.{shortuuid.uuid()}'

        return rmq.RmqThreadCommunicator.connect(
            connection_params={'url': 'amqp://guest:guest@localhost:5672/'},
            message_exchange=message_exchange,
            task_exchange=task_exchange,
            task_queue=task_queue,
            testing_mode=True
        )

    def destroy_communicator(self, communicator):
        communicator.close()

    def test_context_manager(self):
        MESSAGE = 'get this yo'

        rpc_future = kiwipy.Future()

        def rpc_get(_comm, msg):
            rpc_future.set_result(msg)

        self.communicator.add_rpc_subscriber(rpc_get, 'test_context_manager')
        # Check the context manager of the communicator works
        with self.communicator as comm:
            comm.rpc_send('test_context_manager', MESSAGE)

        message = rpc_future.result(self.WAIT_TIMEOUT)
        self.assertEqual(MESSAGE, message)

    def test_custom_task_queue(self):
        """Test creating a custom task queue"""
        TASK = 'The meaning?'
        RESULT = 42
        result_future = kiwipy.Future()

        tasks = []

        def on_task(_comm, task):
            tasks.append(task)
            return result_future

        task_queue = self.communicator.task_queue(f'test-queue-{utils.rand_string(5)}')

        task_queue.add_task_subscriber(on_task)
        task_future = task_queue.task_send(TASK).result(timeout=self.WAIT_TIMEOUT)

        result_future.set_result(42)

        result = task_future.result(timeout=self.WAIT_TIMEOUT)

        self.assertEqual(TASK, tasks[0])
        self.assertEqual(RESULT, result)

    def test_task_queue_next(self):
        """Test creating a custom task queue"""
        TASK = 'The meaning?'
        RESULT = 42

        # Create a new queue and sent the task
        task_queue = self.communicator.task_queue(f'test-queue-{utils.rand_string(5)}')
        task_future = task_queue.task_send(TASK)

        # Get the task and carry it out
        with task_queue.next_task() as task:
            task.process().set_result(RESULT)

        # Now wait for the result
        result = task_future.result(timeout=self.WAIT_TIMEOUT)
        self.assertEqual(RESULT, result)


def test_queue_get_next(thread_task_queue: rmq.RmqThreadTaskQueue):
    """Test getting the next task from the queue"""
    result = thread_task_queue.task_send('Hello!')
    with thread_task_queue.next_task(timeout=1.) as task:
        with task.processing() as outcome:
            assert task.body == 'Hello!'
            outcome.set_result('Goodbye')
    assert result.result() == 'Goodbye'


def test_queue_iter(thread_task_queue: rmq.RmqThreadTaskQueue):
    """Test iterating through a task queue"""
    results = []

    # Insert tasks
    for i in range(10):
        results.append(thread_task_queue.task_send(i))

    for task in thread_task_queue:
        with task.processing() as outcome:
            outcome.set_result(task.body * 10)

    concurrent.futures.wait(results)
    assert all(result.done() for result in results)

    # Make sure there are no more tasks in the queue
    for _ in thread_task_queue:
        assert False, "Shouldn't get here"


def test_queue_iter_not_process(thread_task_queue: rmq.RmqThreadTaskQueue):
    """Check what happens when we iterate a queue but don't process all tasks"""
    outcomes = []

    # Insert tasks
    for i in range(10):
        outcomes.append(thread_task_queue.task_send(i))

    # Now let's see what happens when we have tasks but don't process some of them
    for task in thread_task_queue:
        if task.body < 5:
            task.process().set_result(task.body * 10)

    concurrent.futures.wait(outcomes[:5])
    for i, outcome in enumerate(outcomes[:5]):
        assert outcome.result() == i * 10

    # Now, to through and process the rest
    for task in thread_task_queue:
        task.process().set_result(task.body * 10)

    concurrent.futures.wait(outcomes)
    for i, outcome in enumerate(outcomes):
        assert outcome.result() == i * 10


def test_queue_task_forget(thread_task_queue: rmq.RmqThreadTaskQueue):
    """
    Check what happens when we forget to process a task we said we would
    WARNING: This test mail fail when running with a debugger as it relies on the 'outcome'
    reference count dropping to zero but the debugger may be preventing this.
    """
    outcomes = list()

    outcomes.append(thread_task_queue.task_send(1))

    # Get the first task and say that we will process it
    outcome = None
    with thread_task_queue.next_task() as task:
        outcome = task.process()

    with pytest.raises(kiwipy.exceptions.QueueEmpty):
        with thread_task_queue.next_task():
            pass

    # Now let's 'forget' i.e. lose the outcome
    del outcome

    # Now the task should be back in the queue
    with thread_task_queue.next_task() as task:
        task.process().set_result(10)

    concurrent.futures.wait(outcomes)
    assert outcomes[0].result() == 10


def test_empty_queue(thread_task_queue: rmq.RmqThreadTaskQueue):
    with pytest.raises(kiwipy.exceptions.QueueEmpty):
        with thread_task_queue.next_task(timeout=5.):
            pass


def test_task_processing_exception(thread_task_queue: rmq.RmqThreadTaskQueue):
    """Check that if there is an exception processing a task that it is removed from the queue"""
    task_future = thread_task_queue.task_send('Do this')

    # The error should still get propageted in the 'worker'
    with pytest.raises(RuntimeError):
        with thread_task_queue.next_task(timeout=WAIT_TIMEOUT) as task:
            with task.processing():
                raise RuntimeError('Cannea do it captain!')

    # And the task sender should get a remote exception to inform them of the problem
    with pytest.raises(kiwipy.RemoteException):
        task_future.result(timeout=WAIT_TIMEOUT)

    # The queue should now be empty
    with pytest.raises(kiwipy.QueueEmpty):
        with thread_task_queue.next_task(timeout=1.):
            pass


def test_connection_close_callback():
    """Test that a callback set with `add_close_callback` is correctly called."""
    result = []

    def close_callback(sender, exc):  # pylint: disable=unused-argument
        result.append('called')

    communicator = rmq.connect(
        connection_params={'url': 'amqp://guest:guest@localhost:5672/'},
        message_exchange=f'{__file__}.{shortuuid.uuid()}',
        task_exchange=f'{__file__}.{shortuuid.uuid()}',
        task_queue=f'{__file__}.{shortuuid.uuid()}',
        testing_mode=True
    )
    communicator.add_close_callback(close_callback)
    communicator.close()
    assert result == ['called']


def test_jupyter_notebook():
    """Test that the `RmqThreadCommunicator` can be used in a Jupyter notebook."""
    from pytest_notebook.nb_regression import NBRegressionFixture

    fixture = NBRegressionFixture(exec_timeout=50)
    fixture.diff_color_words = False
    fixture.diff_ignore = ('/metadata/language_info/version',)

    my_dir = pathlib.Path(__file__).parent
    with open(my_dir / pathlib.Path('notebooks/communicator.ipynb')) as handle:
        fixture.check(handle)


def test_server_properties(thread_communicator: kiwipy.rmq.RmqThreadCommunicator):
    props = thread_communicator.server_properties
    assert isinstance(props, dict)

    assert props['product'] == b'RabbitMQ'
    assert 'version' in props
    assert props['platform'].startswith(b'Erlang')


# region Broadcast


def test_broadcast_send(thread_communicator: kiwipy.rmq.RmqThreadCommunicator):
    SUBJECT = 'yo momma'
    BODY = 'so fat'
    SENDER_ID = 'me'
    FULL_MSG = {'body': BODY, 'subject': SUBJECT, 'sender': SENDER_ID, 'correlation_id': None}

    message1 = kiwipy.Future()
    message2 = kiwipy.Future()

    def on_broadcast_1(_comm, body, sender, subject, correlation_id):
        message1.set_result({'body': body, 'subject': subject, 'sender': sender, 'correlation_id': correlation_id})

    def on_broadcast_2(_comm, body, sender, subject, correlation_id):
        message2.set_result({'body': body, 'subject': subject, 'sender': sender, 'correlation_id': correlation_id})

    thread_communicator.add_broadcast_subscriber(on_broadcast_1)
    thread_communicator.add_broadcast_subscriber(on_broadcast_2)

    thread_communicator.broadcast_send(**FULL_MSG)

    assert message1.result() == FULL_MSG
    assert message2.result() == FULL_MSG


def test_broadcast_filter_subject(thread_communicator: kiwipy.rmq.RmqThreadCommunicator):
    subjects = []
    EXPECTED_SUBJECTS = ['purchase.car', 'purchase.piano']

    done = kiwipy.Future()

    def on_broadcast_1(_comm, _body, _sender=None, subject=None, _correlation_id=None):
        subjects.append(subject)
        if len(subjects) == len(EXPECTED_SUBJECTS):
            done.set_result(True)

    thread_communicator.add_broadcast_subscriber(kiwipy.BroadcastFilter(on_broadcast_1, subject='purchase.*'))

    for subj in ['purchase.car', 'purchase.piano', 'sell.guitar', 'sell.house']:
        thread_communicator.broadcast_send(None, subject=subj)

    assert len(subjects) == 2
    assert EXPECTED_SUBJECTS == subjects


def test_broadcast_filter_sender(thread_communicator: kiwipy.rmq.RmqThreadCommunicator):
    EXPECTED_SENDERS = ['bob.jones', 'alice.jones']
    senders = []

    done = kiwipy.Future()

    def on_broadcast_1(_comm, _body, sender=None, _subject=None, _correlation_id=None):
        senders.append(sender)
        if len(senders) == len(EXPECTED_SENDERS):
            done.set_result(True)

    thread_communicator.add_broadcast_subscriber(kiwipy.BroadcastFilter(on_broadcast_1, sender='*.jones'))

    for subj in ['bob.jones', 'bob.smith', 'martin.uhrin', 'alice.jones']:
        thread_communicator.broadcast_send(None, sender=subj)

    assert len(senders) == 2
    assert senders == EXPECTED_SENDERS


def test_broadcast_filter_sender_and_subject(thread_communicator: kiwipy.rmq.RmqThreadCommunicator):
    senders_and_subects = set()
    EXPECTED = {
        ('bob.jones', 'purchase.car'),
        ('bob.jones', 'purchase.piano'),
        ('alice.jones', 'purchase.car'),
        ('alice.jones', 'purchase.piano'),
    }

    done = kiwipy.Future()

    def on_broadcast_1(_comm, _body, sender=None, subject=None, _correlation_id=None):
        senders_and_subects.add((sender, subject))
        if len(senders_and_subects) == len(EXPECTED):
            done.set_result(True)

    filtered = kiwipy.BroadcastFilter(on_broadcast_1)
    filtered.add_sender_filter('*.jones')
    filtered.add_subject_filter('purchase.*')
    thread_communicator.add_broadcast_subscriber(filtered)

    for sender in ['bob.jones', 'bob.smith', 'martin.uhrin', 'alice.jones']:
        for subj in ['purchase.car', 'purchase.piano', 'sell.guitar', 'sell.house']:
            thread_communicator.broadcast_send(None, sender=sender, subject=subj)

    assert len(senders_and_subects) == 4
    assert senders_and_subects == EXPECTED


# def test_add_remove_broadcast_subscriber(connection_params):
#     # Set the expiry to something small so we know that the queues expire after we unsubscribe
#     communicator = await utils.new_communicator(connection_params, settings={'queue_expires': 1})

#     async with communicator:
#         broadcast_received = asyncio.Future()

#         def broadcast_subscriber(_comm, _body, _sender=None, _subject=None, _correlation_id=None):
#             broadcast_received.set_result(True)

#         # Check we're getting messages
#         await communicator.add_broadcast_subscriber(broadcast_subscriber, broadcast_subscriber.__name__)
#         await communicator.broadcast_send(None)
#         assert (await broadcast_received) is True

#         await communicator.remove_broadcast_subscriber(broadcast_subscriber.__name__)
#         # Check that we're unsubscribed
#         broadcast_received = asyncio.Future()
#         with pytest.raises(asyncio.TimeoutError):
#             await asyncio.wait_for(broadcast_received, timeout=2.)

#         # Wait to make sure the queue is expired.  The queue_expires above is in milliseconds while below
#         # it is in seconds so this should be enough for RMQ to get its ass in gear
#         await asyncio.sleep(1.)

#         # Now re-add and check we're getting messages
#         broadcast_received = asyncio.Future()
#         await communicator.add_broadcast_subscriber(broadcast_subscriber, broadcast_subscriber.__name__)
#         await communicator.broadcast_send(None)
#         assert (await broadcast_received) is True

# endregion
