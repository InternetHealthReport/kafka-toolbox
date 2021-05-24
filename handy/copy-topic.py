import argparse
import json
import sys
from datetime import datetime, timezone

from confluent_kafka import Consumer, KafkaException, Producer, \
    OFFSET_BEGINNING, OFFSET_END, TopicPartition, TIMESTAMP_CREATE_TIME
from confluent_kafka.admin import AdminClient, ConfigResource, ConfigSource, \
    NewTopic

# This is the error code for the PARTITION_EOF KafkaError, but for some
# reason it is not defined in the Python enum.
PARTITION_EOF = -191
TIMEOUT_IN_S = 10

start_ts = OFFSET_BEGINNING
end_ts = OFFSET_END
total_partitions = 0


def parse_timestamp(arg: str) -> int:
    if arg.isdigit():
        if len(arg) == 10:
            print('Assuming epoch timestamp in seconds.')
            ret = int(arg) * 1000
        elif len(arg) == 13:
            print('Assuming epoch timestamp in milliseconds.')
            ret = int(arg)
        else:
            raise ValueError('Unknown timestamp format: ' + arg)
    else:
        print('Assuming date timestamp with format %Y-%m-%dT%H:%M.')
        ret = int(datetime.strptime(arg, '%Y-%m-%dT%H:%M')
                  .replace(tzinfo=timezone.utc).timestamp()) * 1000
    return ret


def parse_config(config_file: str) -> dict:
    try:
        with open(config_file, 'r') as f:
            ret = json.load(f)
    except json.JSONDecodeError as e:
        print(f'Failed to load specified configuration: {e}', file=sys.stderr)
        return {'error': 0}
    return ret


def get_topic_config(topic: str, admin_client: AdminClient) -> dict:
    result: dict = admin_client.describe_configs(
        [ConfigResource(ConfigResource.Type.TOPIC, topic)])
    # Only one resource is requested so result has only one entry.
    future = list(result.values())[0]
    try:
        config: dict = future.result()
    except Exception as e:
        print(f'Failed to get topic config: {e}', file=sys.stderr)
        return {'error': 0}
    ret = dict()
    for resource in config.values():
        # No need to store default settings.
        if resource.source == ConfigSource.DEFAULT_CONFIG.value:
            continue
        ret[resource.name] = resource.value
    return ret


def get_replication_factor_and_partition_count(
        topic: str, admin_client: AdminClient) -> (int, int):
    try:
        result = admin_client.list_topics(topic)
    except KafkaException as e:
        print(f'Failed to get replication factor and partition count: {e}',
              file=sys.stderr)
        return -1, -1
    topic = list(result.topics.values())[0]
    partition_count = len(topic.partitions)
    replicas = set([len(r.replicas) for r in topic.partitions.values()])
    if len(replicas) > 1:
        print(f'Warning: Partitions are assigned to different number of '
              f'replicas: {replicas}', file=sys.stderr)
        print(f'Using the maximum value.')
    replication_factor = max(replicas)
    return replication_factor, partition_count


def verify_topic_config(topic: NewTopic, admin_client: AdminClient) -> bool:
    try:
        result = admin_client.create_topics([topic], validate_only=True)
        result[topic.topic].result()
    except KafkaException as e:
        print(f'FAIL: {e}')
        return True
    return False


def create_topic(topic: NewTopic, admin_client: AdminClient) -> bool:
    try:
        result = admin_client.create_topics([topic])
        result[topic.topic].result()
    except KafkaException as e:
        print(f'Topic creation failed: {e}')
        return True
    return False


def on_assign(consumer: Consumer, partitions: list) -> None:
    global total_partitions
    total_partitions = len(partitions)
    print(f'Reading from {total_partitions} partitions')
    for p in partitions:
        p.offset = start_ts
    offsets = consumer.offsets_for_times(partitions)
    consumer.assign(offsets)


def delivery_report(err, msg) -> None:
    if err is not None:
        print(f'Message delivery failed: {err}')


def copy_data(source: str, dest: str, bootstrap_server: str) -> None:
    consumer = Consumer({'bootstrap.servers': bootstrap_server,
                         'group.id': source + '-' + dest + '-copy',
                         'enable.partition.eof': True
                         })
    producer = Producer({'bootstrap.servers': bootstrap_server,
                         'compression.codec': 'lz4',
                         'delivery.report.only.error': True,
                         'queue.buffering.max.messages': 10000000,
                         'queue.buffering.max.kbytes': 4194304,  # 4 GiB
                         'queue.buffering.max.ms': 1000,
                         'batch.num.messages': 1000000
                         })
    perf_start = datetime.now().timestamp()
    msg_count = 0
    try:
        consumer.subscribe([source], on_assign=on_assign)
        paused_partitions = 0
        while True:
            msg = consumer.poll(TIMEOUT_IN_S)
            if msg is None:
                print('Timeout')
                continue
            if msg.error():
                if msg.error().code() == PARTITION_EOF:
                    print(f'Partition {msg.partition()} reached EOF')
                    consumer.pause([TopicPartition(msg.topic(),
                                                   msg.partition())])
                    paused_partitions += 1
                    if paused_partitions >= total_partitions:
                        break
                    else:
                        continue
                print(f'Read error: {msg.error()}')
                break
            ts = msg.timestamp()
            if ts[0] != TIMESTAMP_CREATE_TIME:
                print(f'Message has unexpected timestamp type: {ts[0]}')
                continue
            if end_ts != OFFSET_END and ts[1] >= end_ts:
                print(f'Partition {msg.partition()} reached end of specified '
                      f'time interval')
                consumer.pause([TopicPartition(msg.topic(), msg.partition())])
                paused_partitions += 1
                if paused_partitions >= total_partitions:
                    break
                else:
                    continue
            try:
                producer.produce(dest,
                                 key=msg.key(),
                                 value=msg.value(),
                                 timestamp=ts[1],
                                 on_delivery=delivery_report)
            except BufferError:
                print('Buffer error. Flushing queue...')
                producer.flush(TIMEOUT_IN_S)
                print('Rewriting previous message')
                producer.produce(dest,
                                 key=msg.key(),
                                 value=msg.value(),
                                 timestamp=ts[1],
                                 on_delivery=delivery_report)
            producer.poll(0)
            msg_count += 1
    finally:
        read_fin = datetime.now().timestamp()
        print(f'Finished reading after {read_fin - perf_start:.2f} s')
        consumer.close()
        producer.flush()
    perf_end = datetime.now().timestamp()
    elapsed_time = perf_end - perf_start
    print(f'Copied {msg_count} messages in {elapsed_time:.2f} seconds '
          f'({msg_count / elapsed_time:.2f} msg/s)')


def main() -> None:
    global start_ts, end_ts
    desc = """Copy a Kafka topic and its contents to a new location. All
           messages and timestamps are preserved. The topic configuration as
           well as the topic's replication factor and partition count are
           preserved by default but can be overridden."""
    parser = argparse.ArgumentParser(description=desc)
    parser.add_argument('source', metavar='SOURCE', help='source topic')
    parser.add_argument('dest', metavar='DEST', help='destination topic')
    parser.add_argument('-s', '--server', default='localhost:9092',
                        help='bootstrap server (default: localhost:9092)')
    parser.add_argument('-r', '--replication-factor', type=int,
                        help='replication factor for DEST (default: '
                             'replication factor of SOURCE)')
    parser.add_argument('-p', '--partitions', type=int,
                        help='number of partitions for DEST (default: number '
                             'of partitions of SOURCE)')
    config_group_desc = """By default, the script copies the configuration from
                        SOURCE. This behavior can be overridden by using the 
                        --default flag. New configuration parameters can be
                        passed in a JSON file and the --configuration option.
                        See 
                        https://kafka.apache.org/documentation.html#topicconfigs
                        for a list of valid parameters. These parameters take
                        precedence over parameters copies from SOURCE or 
                        --default."""
    config_group = parser.add_argument_group('Topic configuration',
                                             description=config_group_desc)
    config_group.add_argument('-d', '--default', action='store_true',
                              help='use default configuration settings for '
                                   'DEST')
    config_group.add_argument('-c', '--configuration',
                              help='JSON file with new configuration '
                                   'parameters for DEST')
    read_group_desc = """By using the --start and --end options, only a
                      specific range of SOURCE can be copied. Either or both
                      options can be specified. Timestamps can be specified as 
                      UNIX epoch in (milli)seconds or in the format
                      '%Y-%m-%dT%H:%M'."""
    read_group = parser.add_argument_group('Interval specification',
                                           description=read_group_desc)
    read_group.add_argument('-st', '--start',
                            help='start timestamp (default: read topic from '
                                 'beginning)')
    read_group.add_argument('-e', '--end',
                            help='end timestamp (default: read topic to the '
                                 'end)')

    args = parser.parse_args()

    if args.start:
        start_ts = parse_timestamp(args.start)
    if args.end:
        end_ts = parse_timestamp(args.end)

    admin_client = AdminClient({'bootstrap.servers': args.server})

    user_config = dict()
    if args.configuration:
        user_config = parse_config(args.configuration)
    if 'error' in user_config:
        sys.exit(1)
    if user_config:
        print(f'Applying user-defined topic configuration settings: '
              f'{user_config}')

    topic_config = dict()
    if args.default:
        print('Using default topic configuration settings.')
    else:
        topic_config = get_topic_config(args.source, admin_client)
    if 'error' in topic_config:
        sys.exit(1)
    if topic_config:
        print(f'Applying topic configuration settings derived from SOURCE: '
              f'{topic_config}')
    topic_config.update(user_config)
    if topic_config:
        print(f'Final topic configuration: {topic_config}')

    replication_factor, partition_count = \
        get_replication_factor_and_partition_count(args.source, admin_client)
    if replication_factor < 0 or partition_count < 0:
        sys.exit(1)
    if args.replication_factor:
        replication_factor = args.replication_factor
    if args.partitions:
        partition_count = args.partitions
    print(f'Replication factor: {replication_factor}')
    print(f'Partition count: {partition_count}')

    dest = NewTopic(args.dest, partition_count, replication_factor,
                    config=topic_config)
    print('Verifying topic configuration... ', end='')
    if verify_topic_config(dest, admin_client):
        sys.exit(1)
    else:
        print('OK')

    if create_topic(dest, admin_client):
        sys.exit(1)
    copy_data(args.source, args.dest, args.server)


if __name__ == '__main__':
    main()
    sys.exit(0)
