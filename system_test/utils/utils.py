#!/usr/bin/env python3
import argparse
import hashlib
import os
import re

from confluent_kafka import Producer, Consumer, TopicPartition, Message
from confluent_kafka.admin import AdminClient, NewTopic
import pykafka


def create_topic(topic, partitions, bootstrap_servers):
    a = AdminClient({'bootstrap.servers': bootstrap_servers})
    new_topics = [NewTopic(topic, num_partitions=partitions, replication_factor=1)]
    fs = a.create_topics(new_topics)
    for topic, f in fs.items():
        try:
            f.result()
        except Exception as e:
            print("Failed to create topic {}: {}".format(topic, e))


def gen_message(partition, num, bootstrap_servers, size=10000):
    value = os.getrandom(size)
    checksum = hashlib.md5(value)
    key = f"part_{partition}_num_{num}_{checksum.hexdigest()}"
    return (key, value)


def produce_messages(topic, partition, start_num, count, bootstrap_servers, size=10000):
    producer = Producer({'bootstrap.servers': bootstrap_servers})
    for num in range(start_num, start_num + count):
        if num % 100 == 0:
            print(f"Produced {num - start_num}/{count} messages")
        (key, value) = gen_message(partition, num, size)
        producer.produce(topic, value,
                         key=key,
                         partition=partition)
    producer.flush()


weird_messages = [
    {
        'value': None,
        'key': b'noneValue'
    },
    {
        'value': b'noneKey',
        'key': None
    },
    {
        'value': b'emptyKey',
        'key': b''
    },
    {
        'value': b'',
        'key': b'emptyValue'
    },
    {
        'value': b"normalHeaders",
        'key': b"headers",
        'headers': {
            "foo": b"bar",
            "baz": b"banana",
            "normal": b"someheader",
            "null": None,
            "empty": b'',
            "": "blubb"
        }
    },
    {
        'value': b"nullHeaders",
        'key': b"headers",
        'headers': {
            "null": None,
            "bla": None,
            "foo": b"bar",
        }
    },
    {
        'value': b"emptyHeaders",
        'key': b"headers",
        'headers': {
            "foo": b"bar",
            "empty": b'',
            "blubb": b'',
        }
    },
    {
        'value': b"nonUTFHeaders",
        'key': b"headers",
        'headers': {
            "nonutf8": bytes.fromhex("deadbeef"),
            "nonutf82": bytes.fromhex("0000"),
            "nonutf83": bytes.fromhex("ffff"),
            bytes.fromhex("deadbeef"): bytes.fromhex("deadbeef"),
        }
    }
]


def produce_weird_messages(topic, partition, bootstrap_servers):
    producer = Producer({'bootstrap.servers': bootstrap_servers})
    for msg in weird_messages:
        if 'headers' in msg:
            producer.produce(topic, msg['value'], key=msg['key'], headers=msg['headers'], partition=partition)
        else:
            producer.produce(topic, msg['value'], key=msg['key'], partition=partition)
    producer.flush()
    # pykafka for weird timestamps
    pykafkaClient = pykafka.KafkaClient(hosts=bootstrap_servers)
    pykafkaTopic = pykafkaClient.topics[topic]
    with pykafkaTopic.get_sync_producer() as pykafkaProducer:
        pykafkaProducer.produce(b"noneTimestamp")


def check_msg_equality(msgnum, expected, given: Message, expectFail=False):
    errs = []
    if expected['value'] != given.value():
        errs.append(f"Values do not match! Expected {expected['value']} got {given.value()}")
    if expected['key'] != given.key():
        errs.append(f"Keys do not match! Expected {expected['key']} got {given.key()}")
    if 'headers' in expected and expected['headers'] != given.headers():
        errs.append(f"Headers do not match! Expected {expected['headers']} got {given.headers()}")
    if expectFail:
        if len(errs) == 0:
            print(f"{msgnum}: expected ({expected}) matched given. Did not expect that")
            exit(1)
    elif len(errs) > 0:
        for err in errs:
            print(f"{msgnum}" + err)
        exit(255)


def consume_verify_weird_messages(topic, partition, bootstrap_servers):
    c = Consumer({'bootstrap.servers': bootstrap_servers,
                  'group.id': 'group2',
                  'enable.auto.commit': False,
                  'auto.offset.reset': 'beginning'})
    c.assign([TopicPartition(topic, partition, 0)])

    count = 3
    msgs = []
    while len(msgs) < count:
        msg = c.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print("Consumer error: {}".format(msg.error()))
            exit(255)
        msgs.append(msg)

    c.close()

    for i in range(len(msgs)):
        check_msg_equality(i, weird_messages[i], msgs[i])
        for j in range(len(weird_messages)):
            if i != j:
                check_msg_equality(i, weird_messages[j], msgs[i], expectFail=True)


key_regex = re.compile('part_([0-9]*)_num_([0-9]*)_(.*)')


def verify_messages(expected_num, partition, msg):
    key = msg.key()
    value = msg.value()
    m = key_regex.match(key.decode('utf-8'))
    (key_partition, key_num, key_checksum) = m.groups()
    recordInfo = f"Offset {msg.offset()} Partition {partition}, Key {key_num}, vlength {len(value)}"
    if expected_num != int(key_num):
        print(recordInfo)
        print(f"Number mismatch: Expected {expected_num} got {key_num}")
        exit(255)
    if key_checksum != hashlib.md5(value).hexdigest():
        print(recordInfo)
        print(
            f"Checksum mismatch: Checksum in key ({key_checksum}) does not match Checksum of value {hashlib.md5(value).hexdigest()}")
        exit(255)


def count_messages(bootstrap_servers):
    c = Consumer({'bootstrap.servers': bootstrap_servers,
                  'group.id': 'group2',
                  'enable.auto.commit': False,
                  'auto.offset.reset': 'beginning'})

    metadata = c.list_topics()
    topics = metadata.topics
    for topic, topicMetadata in topics.items():
        for partition in topicMetadata.partitions:
            (low, high) = c.get_watermark_offsets(TopicPartition(topic, partition))
            print(f"{topic} {partition}: {high}")


def consume_verify_messages(topic, partition, start_num, count, bootstrap_servers):
    c = Consumer({'bootstrap.servers': bootstrap_servers,
                  'group.id': 'group2',
                  'enable.auto.commit': False,
                  'auto.offset.reset': 'beginning'})
    c.assign([TopicPartition(topic, partition, 0)])

    num_msg = 0
    while num_msg < count:
        msg = c.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print("Consumer error: {}".format(msg.error()))
            exit(255)
        verify_messages(start_num + num_msg, partition, msg)
        num_msg += 1
    c.close()


def consume_messages(topic, consumer_group, bootstrap_servers, count):
    c = Consumer({'bootstrap.servers': bootstrap_servers,
                  'group.id': consumer_group,
                  'auto.offset.reset': 'beginning'})
    c.subscribe([topic])

    num_msg = 0
    while num_msg < count:
        msg = c.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print("Consumer error: {}".format(msg.error()))
            exit(255)
        num_msg += 1
    c.close()


parser = argparse.ArgumentParser()
parser.add_argument('--bootstrap-servers', type=str, default='localhost:9092')
subparsers = parser.add_subparsers()

p_create_topic = subparsers.add_parser('create_topic')
p_create_topic.add_argument('--topic', type=str, required=True)
p_create_topic.add_argument('--partitions', type=int, required=True)
p_create_topic.set_defaults(func=create_topic)

p_produce_messages = subparsers.add_parser('produce_messages')
p_produce_messages.add_argument('--topic', type=str, required=True)
p_produce_messages.add_argument('--partition', type=int, required=True)
p_produce_messages.add_argument('--start_num', type=int, default=0)
p_produce_messages.add_argument('--count', type=int, required=True)
p_produce_messages.add_argument('--size', type=int, default=10000)
p_produce_messages.set_defaults(func=produce_messages)

p_consume_verify_messages = subparsers.add_parser('consume_verify_messages')
p_consume_verify_messages.add_argument('--topic', type=str, required=True)
p_consume_verify_messages.add_argument('--partition', type=int, required=True)
p_consume_verify_messages.add_argument('--start_num', type=int, default=0)
p_consume_verify_messages.add_argument('--count', type=int, required=True)
p_consume_verify_messages.set_defaults(func=consume_verify_messages)

p_consume_messages = subparsers.add_parser('consume_messages')
p_consume_messages.add_argument('--topic', type=str, required=True)
p_consume_messages.add_argument('--consumer_group', type=str, required=True)
p_consume_messages.add_argument('--count', type=int, required=True)
p_consume_messages.set_defaults(func=consume_messages)

p_produce_weird_messages = subparsers.add_parser('produce_weird_messages')
p_produce_weird_messages.add_argument('--topic', type=str, required=True)
p_produce_weird_messages.add_argument('--partition', type=int, required=True)
p_produce_weird_messages.set_defaults(func=produce_weird_messages)

p_consume_verify_weird_messages = subparsers.add_parser('consume_verify_weird_messages')
p_consume_verify_weird_messages.add_argument('--topic', type=str, required=True)
p_consume_verify_weird_messages.add_argument('--partition', type=int, required=True)
p_consume_verify_weird_messages.set_defaults(func=consume_verify_weird_messages)

p_count_messages = subparsers.add_parser('count_messages')
p_count_messages.set_defaults(func=count_messages)


args = parser.parse_args()
if 'func' not in args:
    parser.parse_args(['--help'])
    exit(1)
func = args.func
fargs = vars(args)
del fargs['func']
func(**fargs)
