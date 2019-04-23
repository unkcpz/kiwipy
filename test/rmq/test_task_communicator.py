import kiwipy

import asynctest
import asyncio
import aio_pika

import shortuuid

from kiwipy import rmq

class TestCoroutineTaskBroker(asynctest.TestCase):

    connection = None

    async def new_task_broker(self, settings=None):
        settings = settings or {}

        task_exchange = "{}.{}".format(self.__class__.__name__, shortuuid.uuid())
        task_queue = "{}.{}".format(self.__class__.__name__, shortuuid.uuid())

        if self.connection is None:
            self.connection = await aio_pika.connect_robust('amqp://guest:guest@localhost:5672/', loop=self.loop)
        communicator = rmq.RmqTaskBroker(
            self.connection,
            task_exchange=task_exchange,
            task_queue=task_queue,
            testing_mode=True,
            **settings,
        )
        await communicator.connect()
        return communicator

    def setUp(self):
        super(TestCoroutineTaskBroker, self).setUp()
        self.loop = asyncio.new_event_loop()
        self.communicator = self.loop.run_until_complete(self.new_task_broker())

    def tearDown(self):
        self.loop.run_until_complete(self.communicator.disconnect())
        super(TestCoroutineTaskBroker, self).tearDown()

    async def test_task_send(self):
        TASK = 'The meaning?'
        RESULT = 42

        tasks = []

        def on_task(_comm, task):
            tasks.append(task)
            return RESULT

        await self.communicator.add_task_subscriber(on_task)
        result_future = await self.communicator.task_send(TASK)
        result = await result_future

        self.assertEqual(tasks[0], TASK)
        self.assertEqual(RESULT, result)

    # async def test_future_task(self):
    #     """
    #     Test a task that returns a future meaning that will be resolve to a value later
    #     """
    #     TASK = 'The meaning?'
    #     RESULT = 42
    #     result_future = kiwipy.Future()
    #
    #     tasks = []
    #
    #     def on_task(_comm, task):
    #         tasks.append(task)
    #         return result_future
    #
    #     await self.communicator.add_task_subscriber(on_task)
    #     task_future = await self.communicator.task_send(TASK)
    #
    #     # The task has given us a future
    #     future_from_task = await task_future
    #     self.assertTrue(asyncio.isfuture(future_from_task))
    #
    #     # Now resolve the future which should give us a result
    #     result_future.set_result(42)
    #
    #     result = await future_from_task
    #
    #     self.assertEqual(tasks[0], TASK)
    #     self.assertEqual(RESULT, result)

    async def test_task_exception(self):
        TASK = 'The meaning?'

        tasks = []

        def on_task(_comm, task):
            tasks.append(task)
            raise RuntimeError("I cannea do it Captain!")

        await self.communicator.add_task_subscriber(on_task)
        with self.assertRaises(kiwipy.RemoteException):
            result_future = await self.communicator.task_send(TASK)
            await result_future

        self.assertEqual(tasks[0], TASK)

    async def test_task_no_reply(self):
        """Test that we don't get a reply if we don't ask for one, i.e. fire-and-forget"""
        TASK = 'The meaning?'  # pylint: disable=invalid-name
        RESULT = 42  # pylint: disable=invalid-name

        tasks = []

        task_future = kiwipy.Future()

        def on_task(_comm, task):
            tasks.append(task)
            task_future.set_result(RESULT)
            return RESULT

        await self.communicator.add_task_subscriber(on_task)
        result = await self.communicator.task_send(TASK, no_reply=True)

        # Make sure the task actually gets done
        yield task_future

        self.assertEqual(1, len(tasks))
        self.assertEqual(tasks[0], TASK)
        self.assertEqual(None, result)
