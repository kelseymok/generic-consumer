import traceback
import logging
from typing import List, Callable

from confluent_kafka import Consumer, KafkaError, KafkaException


class ConsumerStatus:
    def __init__(self):
        self.running: bool = True

def log_message_error(msg):
    error = msg.error()
    if error.code() == KafkaError._PARTITION_EOF:
        # End of partition event
        logging.info('%% %s [%d] reached end at offset %d\n' %
                     (msg.topic(), msg.partition(), msg.offset()))
    raise KafkaException(msg.error())

def basic_consume_loop(consumer: Consumer, topics: List, consumer_status: ConsumerStatus, f: Callable, messages_to_fetch: int=500, timeout: float=0.3):
    try:
        consumer.subscribe(topics)
        print("Start consuming loop for ", topics)
        while consumer_status.running:
            raw_msgs = consumer.consume(num_messages=messages_to_fetch, timeout=timeout)
            print(f"Length raw_msgs: {len(raw_msgs)}")
            msgs = list(filter(lambda msg: msg is not None, raw_msgs))
            print(f"Length msgs: {len(msgs)}")
            errorMgs = list(filter(lambda msg: msg.error() is not None, msgs))
            print(f"Length errorMgs: {len(errorMgs)}")
            okMessages = list(filter(lambda msg: msg.error() is None, msgs))
            print(f"Length okMessages: {len(okMessages)}")

            for errorMsg in errorMgs:
                log_message_error(errorMsg)

            print(len(okMessages))
            for msg in okMessages:
                print(msg)
                decoded_message = msg.value().decode("utf-8")
                f(decoded_message)

        print("End consuming loop")
    except Exception as _:
        print(traceback.format_exc())
    finally:
        consumer.close()

def shutdown(consumer_status: ConsumerStatus):
    consumer_status.running = False