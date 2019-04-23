import kiwipy

import asyncio
import aio_pika
import logging
import sys
import traceback
import uuid
import collections
from collections import deque

from . import defaults
from . import utils

_LOGGER = logging.getLogger(__name__)
__all__ = ['RmqTaskSubscriber']

TaskBody = collections.namedtuple('TaskBody', ('task', 'no_reply'))

class RmqTaskSubscriber(object):
    """
    Listens for tasks comming in on the RMQ task queue
    """
    DEFAULT_EXCHANGE_PARAMS = {'type': aio_pika.ExchangeType.TOPIC}
    TASK_QUEUE_ARGUMENTS = {'x-message-ttl': defaults.TASK_MESSAGE_TTL}

    def __init__(self,
                 connection,
                 exchange_name=defaults.MESSAGE_EXCHANGE,
                 task_queue_name=defaults.TASK_QUEUE,
                 testing_mode=False,
                 prefetch_size=defaults.TASK_PREFETCH_SIZE,
                 prefetch_count=defaults.TASK_PREFETCH_COUNT,
                 decoder=defaults.DECODER,
                 encoder=defaults.ENCODER):
        """
        :param connection: An RMQ connection
        :type connection: :class: `aio_pika.Connection`
        :param exchange_name: the name of the exchange to use
        :type exchange_name: :class:`string`
        :param queue_name: the name of the queue to use
        :type queue_name: :class:`string`
        """
        self._connection = connection
        self._task_queue_name = task_queue_name
        self._testing_mode = testing_mode
        self._subscribers = []
        self._channel = None
        self._prefetch_size = prefetch_size
        self._prefetch_count = prefetch_count

        self._encoder = encoder
        self._decoder = decoder

        self._exchange_name = exchange_name
        self._exchange_params = self.DEFAULT_EXCHANGE_PARAMS

        # create at _create_task_queue
        self._task_queue = None # type: aio_pika.Queue
        self._consumer_tag = None

        # Used in disconnect
        self._is_closing = False

    async def add_task_subscriber(self, subscriber):
        self._subscribers.append(subscriber)
        if self._consumer_tag is None:
            self._consumer_tag = await self._task_queue.consume(self._on_task)

    def remove_task_subscriber(self, subscriber):
        self._subscribers.remove(subscriber)
        if not self._subscribers:
            self._task_queue.cancel(self._consumer_tag)
            self._consumer_tag = None

    async def connect(self):
        if self._channel:
            # Already connected
            return
        self._channel = await self._connection.channel()
        self._exchange = await self._channel.declare_exchange(name=self._exchange_name, **self._exchange_params)
        await self._channel.set_qos(prefetch_count=self._prefetch_count, prefetch_size=self._prefetch_size)

        await self._create_task_queue()

    async def disconnect(self):
        if not self._is_closing:
            self._is_closing = True
            if self._channel is not None:
                await self._channel.close()
                self._channel = None

    async def _create_task_queue(self):
        """Create and bind the task queue"""
        arguments = dict(self.TASK_QUEUE_ARGUMENTS)
        if self._testing_mode:
            arguments['x-expires'] = defaults.TEST_QUEUE_EXPIRES

        # x-expires means how long does the queue stay alive after no clients
        # x-message-ttl means what is the default ttl for a message arriving in the queue
        self._task_queue = await self._channel.declare_queue(
            name=self._task_queue_name,
            durable=not self._testing_mode,
            auto_delete=self._testing_mode,
            arguments=arguments,
        )
        await self._task_queue.bind(self._exchange, routing_key=self._task_queue_name)

    async def _on_task(self, message):
        """
        Used for deal with incoming message

        :param message: The aio_pika RMQ message
        :type message: :class:`aio_pika.IncomingMessage`
        """
        # ignore_processed may reject some message manually
        # import pdb; pdb.set_trace()
        async with message.process(ignore_processed=True):
            handled = False
            # Decode the message tuple into a task body for easier use
            task_body = TaskBody(*self._decoder(message.body)) # type: TaskBody
            for subscriber in self._subscribers:
                try:
                    subscriber = utils.ensure_coroutine(subscriber)
                    result = await subscriber(self, task_body.task)

                    # If a task returns a future it is not considered done until the chain of futures
                    # (i.e. if the first future resolves to a future and so on) finishes and produces
                    # a concrete result
                    while asyncio.isfuture(result):
                        if not task_body.no_reply:
                            await self._send_response(util.pending_response(), message)
                        result = await result
                except kiwipy.TaskRejected:
                    # Keep trying to find one that will accept the task
                    continue
                except kiwipy.CancelledError as exception:
                    if not task_body.no_reply:
                        reply_body = utils.cancelled_response(str(exception))
                    handled = True # Finished
                except KeyboardInterrupt:
                    raise
                except Exception as exc:
                    if task_body.no_reply:
                        # The user has asked for no reply so log an error so they see this exception
                        _LOGGER.exception('The task excepted')
                    else:
                        # Send the exception back to the other side but log here at INFO level also
                        reply_body = utils.exception_response(sys.exc_info()[1:])
                        _LOGGER.info('There was an exception in a task %s: \n%s', exc, traceback.format_exc())
                    handled = True # Finished
                else: # try sucessed
                    # Create a reply message
                    if not task_body.no_reply:
                        reply_body = utils.result_response(result)
                    handled = True

                if handled:
                    message.ack()
                    if not task_body.no_reply:
                        await self._send_response(reply_body, message)
                    break

            if not handled:
                # No one handled the task
                message.reject(requeue=True) # message returned to queue

    def _build_response_message(self, body, incoming_message):
        """
        Create a topika Message as a response to a task being deal with.

        :param body: The message body dictionary
        :type body: dict
        :param incoming_message: The original message we are responding to
        :type incoming_message: :class:`topika.IncomingMessage`
        :return: The response message
        :rtype: :class:`topika.Message`
        """
        # Add host info
        body[utils.HOST_KEY] = utils.get_host_info()
        message = aio_pika.Message(body=self._encoder(body), correlation_id=incoming_message.correlation_id)

        return message

    async def _send_response(self, msg_body, incoming_message):
        # 将得到的结果重新发回
        msg = self._build_response_message(msg_body, incoming_message)
        # import pdb; pdb.set_trace()
        await self._exchange.publish(msg, routing_key=incoming_message.reply_to)

class RmqTaskPublisher(object):
    """
    Publishes messages to the RMQ task queue and gets the response
    """

    DEFAULT_EXCHANGE_PARAMS = {'type': aio_pika.ExchangeType.TOPIC}

    def __init__(self,
                 connection,
                 task_queue_name=defaults.TASK_QUEUE,
                 exchange_name=defaults.MESSAGE_EXCHANGE,
                 exchange_params=None,
                 encoder=defaults.ENCODER,
                 decoder=defaults.DECODER,
                 confirm_deliveries=True,
                 testing_mode=False):
        self._connection = connection
        self._task_queue_name = task_queue_name
        self._exchange_name = exchange_name
        self._exchange_params = self.DEFAULT_EXCHANGE_PARAMS
        self._encoder = encoder
        self._decoder = decoder
        self._confirm_deliveries = confirm_deliveries
        if self._confirm_deliveries:
            self._num_published = 0
            self._delivery_info = deque()

        self._channel = None
        self._testing_mode = testing_mode
        self._awaiting_response = {}

        self._is_closing = False

    async def connect(self):
        if self._channel:
            return

        self._channel = await self._connection.channel(
            publisher_confirms=self._confirm_deliveries,
            on_return_raises=False,
        )
        self._channel.add_close_callback(self._on_channel_close)

        self._exchange = await self._channel.declare_exchange(name=self._exchange_name, **self._exchange_params)

        reply_queue_name = "{}-reply-{}".format(self._exchange_name, str(uuid.uuid4()))
        self._reply_queue = await self._channel.declare_queue(
            name=reply_queue_name,
            exclusive=True,
            auto_delete=self._testing_mode,
            arguments={"x-expires": defaults.REPLY_QUEUE_EXPIRES}
        )

        await self._reply_queue.bind(self._exchange, routing_key=reply_queue_name)
        # 监听从subscriber端处理完并返回的信号
        await self._reply_queue.consume(self._on_response, no_ack=True)

    async def disconnect(self):
        if not self._is_closing:
            self._is_closing = True
            if self._channel is not None:
                await self._channel.close()
                self._channel = None

    def _on_channel_close(self):
        """ Reset all channel specific members """
        if self._confirm_deliveries:
            self._num_published = 0
            self._delivery_info = deque()

    async def task_send(self, task, no_reply=False):
        """
        Send a task for processing by a task subscriber and get response
        :param task: The task payload
        :param no_reply: Don't send a reply containing the result of the task
        :type no_reply: bool
        :return: A future representing the result of the task
        :rtype: :class:`_asyncio.Future`
        """
        # Build the full message body and encoder as a tuple
        body = self._encoder((task, no_reply))
        # Now build up the full aio_pika message
        task_msg = aio_pika.Message(body=body, correlation_id=str(uuid.uuid4()), reply_to=self._reply_queue.name)

        result_future = None
        if no_reply:
            published = await self.publish(task_msg, routing_key=self._task_queue_name, mandatory=True)
        else:
            published, result_future = await self.publish_expect_response(
                task_msg,
                routing_key=self._task_queue_name,
                mandatory=True
            )

        assert published, "The task was not published to the exchange"
        return result_future

    async def _on_response(self, message):
        """
        用subscriber处理完任务后向client发送结果，调用该函数
        Called when we get a message on our response queue

        :param message: The response message
        :type message: :class:`aio_pika.message.IncomingMessage`
        """
        correlation_id = message.correlation_id
        try:
            response_future = self._awaiting_response.pop(correlation_id)
        except KeyError:
            _LOGGER.error("Got a response for an unknown id '%s':\n%s", correlation_id, message)
        else:
            try:
                response = self._decoder(message.body)
            except Exception:
                _LOGGER.error("Failed to decode message body:\n%s%s", message.body, traceback.format_exc())
                raise
            else:
                utils.response_to_future(response, response_future)
                try:
                    # If the response was a future it means we should another message that
                    # resolve that future
                    if concurrent.is_future(response):
                        self._awaiting_response[correlation_id] = response.result()
                except Exception:
                        pass

    async def publish(self, message, routing_key, mandatory=True):
        """
        Send a fire-and-forget message i.e. no response expected.

        :param message: The message to send
        :param routing_key: The routing key
        :param mandatory: If the message cannot be routed this will raise an UnroutableException
        :return:
        """
        result = await self._exchange.publish(
            message, routing_key=routing_key, mandatory=mandatory)
        return result

    async def publish_expect_response(self, message, routing_key, mandatory=True):
        # If there is no correlation id we have to set on so that we know what the response will be to
        if not message.correlation_id:
            message.correlation_id = str(uuid.uuid4()).encode()
        correlation_id = message.correlation_id

        response_future = asyncio.Future()
        self._awaiting_response[correlation_id] = response_future
        result = await self.publish(message, routing_key=routing_key, mandatory=mandatory)
        return result, response_future
