import kiwipy

import aio_pika
import logging
from . import defaults
from . import tasks
from . import utils

__all__ = ['RmqTaskBroker']

_LOGGER = logging.getLogger(__name__)

class RmqMessageBroker(object):

    def __init__(self):
        pass

class RmqTaskBroker(object):

    def __init__(self,
                 connection,
                 task_exchange=defaults.TASK_EXCHANGE,
                 task_queue=defaults.TASK_QUEUE,
                 testing_mode=False):
        self._connection = connection
        self._task_subscriber = tasks.RmqTaskSubscriber(
            connection,
            exchange_name=task_exchange,
            task_queue_name=task_queue,
            testing_mode=testing_mode,
        )
        self._task_publisher = tasks.RmqTaskPublisher(
            connection,
            exchange_name=task_exchange,
            task_queue_name=task_queue,
            testing_mode=testing_mode,
        )


    async def connect(self):
        if self._connection.is_closed:
            await self._connection.connect()

        await self._task_subscriber.connect()
        await self._task_publisher.connect()

    async def disconnect(self):
        await self._task_subscriber.disconnect()
        await self._task_publisher.disconnect()
        await self._connection.close()

    async def add_task_subscriber(self, subscriber):
        await self._task_subscriber.add_task_subscriber(subscriber)

    async def task_send(self, task, no_reply=False):
        try:
            result = await self._task_publisher.task_send(task, no_reply)
            return result
        except pika.exceptions.UnroutableError as exception:
            raise kiwipy.UnroutableError(str(exception))
        except pika.exceptions.NackError as exception:
            raise kiwipy.TaskRejected(str(exception))
