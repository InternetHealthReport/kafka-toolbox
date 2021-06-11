import argparse
import bz2
import os
import pickle
import sys
from datetime import datetime, timezone

from confluent_kafka import Consumer, KafkaException, TopicPartition, \
    OFFSET_BEGINNING, OFFSET_END, TIMESTAMP_CREATE_TIME
from confluent_kafka.admin import AdminClient, ConfigResource, ConfigSource

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


def on_assign(consumer: Consumer, partitions: list) -> None:
    global total_partitions
    total_partitions = len(partitions)
    print(f'Reading from {total_partitions} partitions')
    for p in partitions:
        p.offset = start_ts
    offsets = consumer.offsets_for_times(partitions)
    consumer.assign(offsets)


def dump_data(topic: str, bootstrap_server: str) -> list:
    ret = list()
    consumer = Consumer({'bootstrap.servers': bootstrap_server,
                         'group.id': topic + '-dump',
                         'enable.partition.eof': True
                         })
    perf_start = datetime.now().timestamp()
    try:
        consumer.subscribe([topic], on_assign=on_assign)
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
            if ts[1] < start_ts:
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
            ret.append((msg.key(), msg.value(), ts[1]))
    finally:
        consumer.close()
    elapsed_time = datetime.now().timestamp() - perf_start
    print(f'Read {len(ret)} messages in {elapsed_time:.2f} seconds '
          f'({len(ret) / elapsed_time:.2f} msg/s)')
    return ret


def main() -> None:
    global start_ts, end_ts
    parser = argparse.ArgumentParser()
    parser.add_argument('topic', metavar='TOPIC')
    parser.add_argument('-s', '--server', default='localhost:9092',
                        help='bootstrap server (default: localhost:9092)')
    parser.add_argument('-o', '--output', default='./',
                        help='specify output directory (default: ./)')
    read_group_desc = """By using the --start and --end options, only a
                      specific range of TOPIC can be dumped. Either or both
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

    output_dir = args.output
    if not output_dir.endswith('/'):
        output_dir += '/'
    output = output_dir + args.topic + '.pickle.bz2'

    if args.start:
        start_ts = parse_timestamp(args.start)
    if args.end:
        end_ts = parse_timestamp(args.end)

    dump = {'name': args.topic,
            'start_ts': start_ts,
            'end_ts': end_ts}

    admin_client = AdminClient({'bootstrap.servers': args.server})

    topic_config = get_topic_config(args.topic, admin_client)
    if 'error' in topic_config:
        sys.exit(1)
    dump['config'] = topic_config
    if topic_config:
        print(f'Storing topic configuration: {topic_config}')

    replication_factor, partition_count = \
        get_replication_factor_and_partition_count(args.topic, admin_client)
    if replication_factor < 0 or partition_count < 0:
        sys.exit(1)
    print(f'Replication factor: {replication_factor}')
    print(f'Partition count: {partition_count}')
    dump['replication_factor'] = replication_factor
    dump['partition_count'] = partition_count

    dump['messages'] = dump_data(args.topic, args.server)

    if not args.start:
        dump['start_ts'] = dump['messages'][0][2]
    if not args.end:
        dump['end_ts'] = dump['messages'][-1][2]

    print(f'Compressing to {output}')
    perf_start = datetime.now().timestamp()
    with bz2.open(output, 'wb') as f:
        pickle.dump(dump, f)
    elapsed_time = datetime.now().timestamp() - perf_start
    print(f'Compression took {elapsed_time:.2f} seconds (size: {os.path.getsize(output) / 1024:.0f} kiB)')


if __name__ == '__main__':
    main()
    sys.exit(0)
