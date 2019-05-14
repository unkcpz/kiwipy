..  _user:

Here the simple examples cover the basics of creating messaging applications using kiwipy over RabbitMQ.

The parameters section will focus on some of the important parameters that initialize the communicator and their meaning.

***************
Simple examples
***************

RPC
---

The client:

.. code-block:: python

    from kiwipy import rmq

    communicator = rmq.RmqThreadCommunicator.connect(connection_params={'url': 'amqp://127.0.0.1'})

    # Send an RPC message identifier 'fib'
    print(" [x] Requesting fib(30)")
    response = communicator.rpc_send('fib', 30).result()
    print((" [.] Got %r" % response))

    # Send an RPC message identifier 'fac'
    print(" [x] Requesting fac(3)")
    response = communicator.rpc_send('fac', 3).result()
    print((" [.] Got %r" % response))

`(rmq_rpc_client.py source) <https://raw.githubusercontent.com/muhrin/kiwipy/develop/examples/rmq_rpc_client.py>`_


The server:

.. code-block:: python

    import threading

    from kiwipy import rmq


    def fib(comm, num):
        if num == 0:
            return 0
        if num == 1:
            return 1
        return fib(comm, num - 1) + fib(comm, num - 2)

    def fac(comm, n):
        z=1
        if n>1:
            z=n*fac(comm, n-1)
        return z

    try:
        with rmq.RmqThreadCommunicator.connect(connection_params={'url': 'amqp://127.0.0.1'}) as communicator:
            # Register an RPC subscriber with the name 'fib'
            communicator.add_rpc_subscriber(fib, 'fib')
            # Register an RPC subscriber with the name 'fac'
            communicator.add_rpc_subscriber(fac, 'fac')
            # Now wait indefinitely for fibonacci calls
            threading.Event().wait()
    except KeyboardInterrupt:
        pass

`(rmq_rpc_server.py source) <https://raw.githubusercontent.com/muhrin/kiwipy/develop/examples/rmq_rpc_server.py>`_


Worker
------

Create a new task:

.. code-block:: python

    import sys

    from kiwipy import rmq

    # pylint: disable=invalid-name

    message = ' '.join(sys.argv[1:]) or "Hello World!"

    with rmq.RmqThreadCommunicator.connect(connection_params={'url': 'amqp://127.0.0.1'}) as communicator:
        result = communicator.task_send(message).result(timeout=5.0)
        print(result)

`(rmq_new_task.py source) <https://raw.githubusercontent.com/muhrin/kiwipy/develop/examples/rmq_new_task.py>`_


And the worker:

.. code-block:: python

    import time
    import threading

    from kiwipy import rmq

    print(' [*] Waiting for messages. To exit press CTRL+C')


    def callback(_comm, task):
        print((" [x] Received %r" % task))
        time.sleep(task.count('.'))
        print(" [x] Done")
        return task


    with rmq.RmqThreadCommunicator.connect(connection_params={'url': 'amqp://127.0.0.1/'}) as communicator:
        communicator.add_task_subscriber(callback)
        threading.Event().wait()

`(rmq_worker.py source) <https://raw.githubusercontent.com/muhrin/kiwipy/develop/examples/rmq_worker.py>`_

Broadcast
---------

The Client:

.. code-block:: python

    import sys

    from kiwipy import rmq

    # pylint: disable=invalid-name

    body = ' '.join(sys.argv[1:]) or "___"

    with rmq.RmqThreadCommunicator.connect(connection_params={'url': 'amqp://127.0.0.1'}) as communicator:
        # send message with different sender and subject

        # listen by two subscriber
        sendr = 'bob.jones'
        subj = 'purchase.car'
        communicator.broadcast_send(body, sender=sendr, subject=subj)
        # sender filtered
        sendr = 'bob.smith'
        subj = 'purchase.car'
        communicator.broadcast_send(body, sender=sendr, subject=subj)
        # subject filterd
        sendr = 'bob.jones'
        subj = 'sell.car'
        communicator.broadcast_send(body, sender=sendr, subject=subj)

`(rmq_broadcast_client.py source) <https://raw.githubusercontent.com/muhrin/kiwipy/develop/examples/rmq_broadcast_client.py>`_

The Server:

.. code-block:: python

    import threading

    import kiwipy
    from kiwipy import rmq

    def on_broadcast_send(comm, body, sender, subject, correlation_id):
        print(" [x] on_broadcast_send listening:")
        print(" body: {}, sender {}, subject {}\n".format(body, sender, subject))

    def on_broadcast_filter(comm, body, sender=None, subject=None, correlation_id=None):
        print(" [x] on_broadcast_filter listening:")
        print(" body: {}, sender {}, subject {}\n".format(body, sender, subject))
    filtered = kiwipy.BroadcastFilter(on_broadcast_filter)
    filtered.add_sender_filter("*.jones")
    filtered.add_subject_filter("purchase.*")

    try:
        with rmq.RmqThreadCommunicator.connect(connection_params={'url': 'amqp://127.0.0.1'}) as communicator:
            # Register an broadcast subscriber
            communicator.add_broadcast_subscriber(on_broadcast_send)
            # Register an broadcast subscriber
            communicator.add_broadcast_subscriber(filtered)
            # Now wait indefinitely for fibonacci calls
            threading.Event().wait()
    except KeyboardInterrupt:
        pass

`(rmq_broadcast_server.py source) <https://raw.githubusercontent.com/muhrin/kiwipy/develop/examples/rmq_broadcast_server.py>`_

*********************************
Parameters of communicator
*********************************


* `connection_params`
* `connection_factory`,
* `loop`,
* `message_exchange`
* `task_exchange`
* `task_queue`
* `task_prefetch_size`
* `task_prefetch_count`
* `encoder`
* `decoder`
* `testing_mode`
