#!/usr/bin/env python3
"""
This will subscribe to the ldms msg bus, and post to kafka

Possibly has too many, and too complex args, so it should probably read them from a config file

Sample command line:
./ldms_msg_to_kafka.py \
    --ldms_xprt sock \
    --ldms_host localhost \
    --ldms_port 60001 \
    --ldms_auth ovis \
    --ldms_auth_opts conf=/ldms-auth-omni/omni.ldmsauth.conf \
    --ldms_msg_chan nersc \
    --kafka-servers cluster-kafka-bootstrap.sma.svc.cluster.local \
    --kafka-port 9092 \
    --kafka-topic nersc-json
"""
import sys
import logging
import time
import click
import json
from ovis_ldms import ldms
from kafka import KafkaProducer
from kafka.errors import KafkaError, KafkaTimeoutError, NoBrokersAvailable

logging.basicConfig(level=logging.INFO)
kafka_logger = logging.getLogger('kafka')
kafka_logger.setLevel(logging.WARN)

# Backoff settings for waiting on ldmsd / kafka to become available
CONNECT_RETRY_INITIAL = 2
CONNECT_RETRY_MAX = 30


def on_send_success(record_metadata):
    """callback fired when kafka acks a message"""
    logging.debug(
        f"Sent to topic {record_metadata.topic} "
        f"partition {record_metadata.partition} "
        f"at offset {record_metadata.offset}"
    )


def on_send_error(excp):
    """callback fired when a send ultimately fails"""
    logging.error("Kafka send failed", exc_info=excp)


def connect_ldms(ldms_xprt, ldms_host, ldms_port, ldms_auth, auth_opts, ldms_msg_chan):
    """Connect and subscribe to ldmsd, retrying with backoff until it succeeds."""
    delay = CONNECT_RETRY_INITIAL
    while True:
        try:
            mc = ldms.MsgClient(".*", True)
            x = ldms.Xprt(name=ldms_xprt, auth=ldms_auth, auth_opts=auth_opts)
            x.connect(host=ldms_host, port=ldms_port)
            x.msg_subscribe(ldms_msg_chan, True)
            logging.info("LDMS connected and subscribed")
            return mc, x
        except Exception as e:  # pylint: disable=broad-except
            logging.error(
                f"LDMS connect failed (type={type(e).__name__}): {e}; "
                f"retrying in {delay}s"
            )
            time.sleep(delay)
            delay = min(delay * 2, CONNECT_RETRY_MAX)


def create_kafka_producer(kafka_servers, kafka_port):
    """Create the KafkaProducer, retrying with backoff if no brokers are available yet."""
    delay = CONNECT_RETRY_INITIAL
    while True:
        try:
            return KafkaProducer(
                bootstrap_servers=f"{kafka_servers}:{kafka_port}",
                client_id="ldms_msg_to_kafka",
                # For String:
                #value_serializer=lambda s: s.encode('utf-8')
                # For JSON:
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                buffer_memory=64 * 1024 * 1024,  # cap the producer's internal send buffer
                max_block_ms=30000,              # how long send() blocks once buffer is full
                retries=5,
                linger_ms=20,                    # small batching window improves throughput
            )
        except NoBrokersAvailable as e:
            logging.error(f"Kafka brokers not available yet: {e}; retrying in {delay}s")
            time.sleep(delay)
            delay = min(delay * 2, CONNECT_RETRY_MAX)


@click.command()
@click.option('--ldms_xprt', help='ldmsd xprt [sock]', type=click.STRING, required=True)
@click.option('--ldms_host', help='ldmsd hostname', type=click.STRING, required=True)
@click.option('--ldms_port', help='ldmsd listening port', type=click.INT, required=True)
@click.option('--ldms_auth', help='ldmsd auth [munge|ovis]', type=click.STRING, required=True)
@click.option('--ldms_auth_opts', help='ldmsd auth_opt [socket=<path>|conf=<path>]', type=click.STRING, required=True) # pylint: disable=line-too-long
@click.option('--ldms_msg_chan', help='ldmsd msg channel', type=click.STRING, required=True) # pylint: disable=line-too-long
@click.option('--kafka-servers', help='kafka bootstrap host', type=click.STRING, required=True) # pylint: disable=line-too-long
@click.option('--kafka-port', help='kafka bootstrap port', type=click.INT, default="9092") # pylint: disable=line-too-long
@click.option('--kafka-topic', help='kafka topic', type=click.STRING, required=True) # pylint: disable=line-too-long
def main(ldms_xprt: str, ldms_host: str, ldms_port: int, ldms_auth: str, ldms_auth_opts: str, ldms_msg_chan: str, kafka_servers: str, kafka_port: int, kafka_topic: str):  # pylint: disable=too-many-arguments,line-too-long,undefined-variable
    """
    listen to the ldmsd msg bus for a specific channel
    post each msg to a kafka topic
    """

    # Split ldms_auth_opts (string with comma seperated key=value pairs) into dict
    auth_opts = {}
    for opt in ldms_auth_opts.split(','):
        if '=' in opt:
            key, value = opt.split('=', 1)
            auth_opts[key] = value

    logging.info(
        "START: ldms_msg_to_kafka\n"
        f"\tldms_xprt:{ldms_xprt}\n"
        f"\tldms_host:{ldms_host}\n"
        f"\tldms_port:{ldms_port}\n"
        f"\tldms_auth:{ldms_auth}\n"
        f"\tauth_opts:{auth_opts}\n"
        f"\tldms_msg_chan:{ldms_msg_chan}\n"
        f"\tkafka_servers:{kafka_servers}\n"
        f"\tkafka_port:{kafka_port}\n"
        f"\tkafka_topic:{kafka_topic}\n"
    )

    logging.info("Connecting to LDMS xprt...")
    mc, x = connect_ldms(ldms_xprt, ldms_host, ldms_port, ldms_auth, auth_opts, ldms_msg_chan)

    logging.info("Creating Kafka producer...")
    producer = create_kafka_producer(kafka_servers, kafka_port)
    logging.info("Kafka producer created")

    try:
        logging.info("Entering main loop")
        while True:
            d = mc.get_data()
            while d is None:
                time.sleep(0.25)
                d = mc.get_data()
            logging.info(d.data)

            if d.name != ldms_msg_chan:
                continue

            # Send message asynchronously; callbacks handle success/failure
            # without blocking the loop on producer.send().get()
            try:
                producer.send(kafka_topic, d.data) \
                    .add_callback(on_send_success) \
                    .add_errback(on_send_error)
            except KafkaTimeoutError as e:
                # raised if send() blocks past max_block_ms because the
                # buffer is full / broker unreachable
                logging.critical(f"Kafka send blocked/timed out: {e}")
            except KafkaError as e:
                logging.critical(f"Kafka send failed: {e}")

    except KeyboardInterrupt:
        logging.warning("Exiting, flushing and closing producer...")

    producer.flush(timeout=10)
    producer.close(timeout=10)


if __name__ == "__main__":
    main()  # pylint: disable=no-value-for-parameter
