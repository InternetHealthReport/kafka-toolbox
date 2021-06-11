import argparse
import bz2
import json
import pickle
import sys
from datetime import datetime, timezone

from confluent_kafka import Producer, KafkaException, OFFSET_BEGINNING, \
    OFFSET_END
from confluent_kafka.admin import AdminClient, NewTopic

DATE_FMT = '%Y-%m-%dT%H:%M:%S'
TIMEOUT_IN_S = 10

start_ts = OFFSET_BEGINNING
end_ts = OFFSET_END


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


def delivery_report(err, msg) -> None:
    if err is not None:
        print(f'Message delivery failed: {err}')


def load_data(topic: str, messages: list, bootstrap_server: str) -> None:
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
        # msg is a tuple (key, value, timestamp) where timestamp is
        # UNIX epoch in milliseconds.
        for (key, value, timestamp) in messages:
            if timestamp < start_ts or \
                    (end_ts != OFFSET_END and timestamp >= end_ts):
                continue
            try:
                producer.produce(topic,
                                 key=key,
                                 value=value,
                                 timestamp=timestamp,
                                 on_delivery=delivery_report)
            except BufferError:
                print('Buffer error. Flushing queue...')
                producer.flush(TIMEOUT_IN_S)
                print('Rewriting previous message')
                producer.produce(topic,
                                 key=key,
                                 value=value,
                                 timestamp=timestamp,
                                 on_delivery=delivery_report)
            producer.poll(0)
            msg_count += 1
    finally:
        producer.flush()
    elapsed_time = datetime.now().timestamp() - perf_start
    print(f'Copied {msg_count} messages in {elapsed_time:.2f} seconds '
          f'({msg_count / elapsed_time:.2f} msg/s)')


def main() -> None:
    global start_ts, end_ts
    parser = argparse.ArgumentParser()
    parser.add_argument('input', metavar='INPUT', help='*.pickle.bz2 dump file')
    parser.add_argument('-s', '--server', default='localhost:9092',
                        help='bootstrap server (default: localhost:9092)')
    parser.add_argument('-t', '--topic', help='override topic name')
    parser.add_argument('-r', '--replication-factor', type=int,
                        help='override replication factor')
    parser.add_argument('-p', '--partitions', type=int,
                        help='override partition count')
    config_group_desc = """By default, the script uses the configuration
                        specified in the dump. This behavior can be overridden
                        by using the --default flag. New configuration
                        parameters can be passed in a JSON file and the
                        --configuration option. See 
                        https://kafka.apache.org/documentation.html#topicconfigs
                        for a list of valid parameters. These parameters take
                        precedence over parameters from the dump or
                        --default."""
    config_group = parser.add_argument_group('Topic configuration',
                                             description=config_group_desc)
    config_group.add_argument('-d', '--default', action='store_true',
                              help='use default configuration settings')
    config_group.add_argument('-c', '--configuration',
                              help='JSON file with new configuration '
                                   'parameters')
    read_group_desc = """By using the --start and --end options, only a
                      specific range of INPUT can be restored. Either or both
                      options can be specified. Timestamps can be specified as 
                      UNIX epoch in (milli)seconds or in the format
                      '%Y-%m-%dT%H:%M'."""
    read_group = parser.add_argument_group('Interval specification',
                                           description=read_group_desc)
    read_group.add_argument('-st', '--start',
                            help='start timestamp')
    read_group.add_argument('-e', '--end',
                            help='end timestamp')

    args = parser.parse_args()

    if args.start:
        start_ts = parse_timestamp(args.start)
    if args.end:
        end_ts = parse_timestamp(args.end)

    user_config = dict()
    if args.configuration:
        user_config = parse_config(args.configuration)
    if 'error' in user_config:
        sys.exit(1)

    print(f'Loading dump {args.input}')
    dump = pickle.load(bz2.BZ2File(args.input, 'rb'))
    print(f'             Topic: {dump["name"]}')
    start_ts_str = datetime.fromtimestamp(dump['start_ts'] / 1000,
                                          tz=timezone.utc).strftime(DATE_FMT)
    end_ts_str = datetime.fromtimestamp(dump['end_ts'] / 1000,
                                        tz=timezone.utc).strftime(DATE_FMT)
    print(f'             Start: {start_ts_str}')
    print(f'               End: {end_ts_str}')
    print(f'Replication factor: {dump["replication_factor"]}')
    print(f'   Partition count: {dump["partition_count"]}')
    print(f'     Configuration: {dump["config"]}')
    print(f'          Messages: {len(dump["messages"])}')

    topic_name = dump['name']
    if args.topic:
        topic_name = args.topic
        print(f'Overriding topic name: {topic_name}')

    replication_factor = dump['replication_factor']
    if args.replication_factor:
        replication_factor = args.replication_factor
        print(f'Overriding replication factor: {replication_factor}')

    partition_count = dump['partition_count']
    if args.partitions:
        partition_count = args.partitions
        print(f'Overriding partition count: {partition_count}')

    configuration = dict()
    if args.default:
        print('Using default topic configuration settings.')
    else:
        configuration = dump['config']
    if user_config:
        print(f'Overriding topic configuration: {user_config}')
        configuration.update(user_config)
    if configuration:
        print(f'Final topic configuration: {configuration}')

    if args.start:
        start_ts_str = datetime.fromtimestamp(start_ts / 1000,
                                              tz=timezone.utc) \
            .strftime(DATE_FMT)
        print(f'Loading messages from: {start_ts_str}')
    if args.end:
        end_ts_str = datetime.fromtimestamp(end_ts / 1000, tz=timezone.utc) \
            .strftime(DATE_FMT)
        print(f'  Loading messages to: {end_ts_str}')

    admin_client = AdminClient({'bootstrap.servers': args.server})

    dest = NewTopic(topic_name, partition_count, replication_factor,
                    config=configuration)
    print('Verifying topic configuration... ', end='')
    if verify_topic_config(dest, admin_client):
        sys.exit(1)
    else:
        print('OK')

    if create_topic(dest, admin_client):
        sys.exit(1)

    load_data(topic_name, dump['messages'], args.server)


if __name__ == '__main__':
    main()
    sys.exit(0)
