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
