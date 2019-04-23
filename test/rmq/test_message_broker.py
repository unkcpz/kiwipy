import aio_pika
import asyncio
import asynctest
import shortuuid

from kiwipy import rmq

class TestCoroutineMessageBroker(asynctest.TestCase):

    connection = None

    def test_a(self):
        pass

    async def new_message_broker(self):
        message_exchange = "{}.{}".format(self.__class__.__name__, shortuuid.uuid())

        if self.connection is None:
            self.connection = await aio_pika.connect_robust('amqp://guest:guest@localhost:5672/', loop=self.loop)

        broker = rmq.RmqMessageBroker(
            self.connection,
            message_exchange=message_exchange,
            testing_mode=True,
        )
        await broker.connect()
        return broker

    def setUp(self):
        super(TestCoroutineMessageBroker, self).setUp()
        self.loop = asyncio.new_event_loop()
        self.broker = self.loop.run_until_complete(self.new_message_broker())

    def tearDown(self):
        self.loop.run_until_complete(self.broker.disconnect())

    async def test_rpc_send_receive(self):
        MESSAGE = "sup yo"
        RESPONSE = "nuthin bra"

        messages = []

        def on_receive(_comm, msg):
            messages.append(msg)
            return RESPONSE

        await self.broker.add_rpc_subscriber(on_receive, 'rpc')
        response_future = await self.broker.rpc_send('rpc', MESSAGE)
        response = await response_future

        self.assertEqual(messages[0], MESSAGE)
        self.assertEqual(response, RESPONSE)
