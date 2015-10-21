from application.routes import app
from application.listener import message_received, listen
import kombu
from kombu.common import maybe_declare
from amqp import AccessRefused
import logging


def setup_incoming(hostname):
    connection = kombu.Connection(hostname=hostname)
    connection.connect()

    exchange = kombu.Exchange(type="topic", name="new.bankruptcy")

    channel = connection.channel()

    exchange.maybe_bind(channel)
    maybe_declare(exchange, channel)

    queue = kombu.Queue(name='simple', exchange=exchange, routing_key='#')
    queue.maybe_bind(channel)
    try:
        queue.declare()
    except AccessRefused:
        logging.error("Access Refused")
    logging.debug("queue name, exchange, binding_key: %s, %s, %s", queue.name, queue.exchange, queue.routing_key)

    consumer = kombu.Consumer(channel, queues=queue, callbacks=[message_received], accept=['json'])
    consumer.consume()

    logging.debug('channel_id: %s', consumer.channel.channel_id)
    logging.debug('queue(s): %s', consumer.queues)
    return connection, consumer


def setup_error_queue(hostname):
    connection = kombu.Connection(hostname=hostname)
    producer = connection.SimpleQueue('sync_error')
    return producer


def run():
    logging.info('Run')
    hostname = "amqp://{}:{}@{}:{}".format(app.config['MQ_USERNAME'], app.config['MQ_PASSWORD'],
                                           app.config['MQ_HOSTNAME'], app.config['MQ_PORT'])
    incoming_connection, incoming_consumer = setup_incoming(hostname)
    error_producer = setup_error_queue(hostname)

    listen(incoming_connection, error_producer)
    incoming_consumer.close()
