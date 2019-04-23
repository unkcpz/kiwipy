import kiwipy
import aio_pika
import pika

from functools import partial

from . import defaults

__all__ = ['RmqMessageSubscriber']

class RmqMessageSubscriber(object):
    """
    Subscriber for receiving a rnge of message over RMQ
    """

    DEFAULT_EXCHANGE_PARAMS = {'type': aio_pika.ExchangeType.TOPIC}
    TASK_QUEUE_ARGUMENTS = {'x-message-ttl': defaults.TASK_MESSAGE_TTL}

    def __init__(self,
                 connection,
                 message_exchange=defaults.MESSAGE_EXCHANGE,
                 queue_expires=defaults.QUEUE_EXPIRES,
                 decoder=defaults.DECODER,
                 encoder=defaults.ENCODER,
                 testing_mode=False):
        """
        Subscribes and listens for process control messages and acts on them
        by calling the corresponding methods of the process manager.

        :param connection: The aio_pika connection
        :type connection: :class:`aio_pika.Connection`
        :param message_exchange: The name of the exchange to use
        :param queue_expires: the expiry time for standard queues in milliseconds.  This is the time after which, if
            there are no subscribers, a queue will automatically be deleted by RabbitMQ.
        :type queue_expires: int
        :param encoder: The encoder to call for encoding a message
        :param decoder: The decoder to call for decoding a message
        :param testing_mode: Run in testing mode: all queues and exchanges
            will be temporary
        """
        super(RmqMessageSubscriber, self).__init__()

        self._connection = connection
        self._testing_mode = testing_mode
        self._channel = None
        self._exchange = None
        self._exchange_name = message_exchange

        self._rpc_subscribers = {}

        self._rmq_queue_arguments = dict(self.TASK_QUEUE_ARGUMENTS)
        if queue_expires:
            self._rmq_queue_arguments['x-expires'] = queue_expires

        self._is_closing = False

    async def connect(self):
        if self._channel:
            return

        exchange_params = self.DEFAULT_EXCHANGE_PARAMS

        if self._testing_mode:
            exchange_params.setdefault('auto_delete', self._testing_mode)

        self._channel = await self._connection.channel()
        self._exchange = await self._channel.declare_exchange(name=self._exchange_name, **exchange_params)

    async def disconnect(self):
        if not self._is_closing:
            self._is_closing = True
            if self._channel is not None:
                await self._channel.close()
                self._channel = None
                self._exchange = None

    async def add_rpc_subscriber(self, subscriber, identifier=None):
        # Create RPC queue
        rpc_queue = await self._channel.declare_queue(exclusive=True, arguments=self._rmq_queue_arguments)
        try:
            identifier = await rpc_queue.consume(partial(self._on_rpc, subscriber), consumer_tag=identifier)
        except pika.exceptions.DuplicateConsumerTag:
            raise kiwipy.DuplicateSubscriberIdentifier("RPC identifier '{}'".format(identifier))
        else:
            await rpc_queue.bind(self._exchange, routing_key='{}.{}'.format(defaults.RPC_TOPIC, identifier))
            # save the queue so we can cancel and unbind later
            self._rpc_subscribers[identifier] = rpc_queue
            return identifier


    async def _on_rpc(self, subscriber, message):
        pass
