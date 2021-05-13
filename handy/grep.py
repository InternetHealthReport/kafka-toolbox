import argparse
from datetime import datetime, timezone
import msgpack
import confluent_kafka
from confluent_kafka import Consumer, TopicPartition

timestamp = None
start_ts = confluent_kafka.OFFSET_BEGINNING
end_ts = confluent_kafka.OFFSET_END
partition_total = 0


def parse_timestamp(arg: str) -> int:
        ret = 0
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


def on_assign(consumer: Consumer, partitions: list) -> None:
    global partition_total
    partition_total = len(partitions)
    for p in partitions:
        p.offset = start_ts
    offsets = consumer.offsets_for_times(partitions)
    consumer.assign(offsets)


if __name__ == '__main__':
    text = """Retrieve and print messages in the given topic with the
           possibility to apply filters."""

    parser = argparse.ArgumentParser(description=text)
    parser.add_argument("--topic", "-t", help="Selected topic", required=True)
    parser.add_argument("--num_msg", "-n", help="Number of messages to print",
                        type=int)
    parser.add_argument("--server", "-s", help="Bootstrap server",
                        default='kafka1:9092')
    parser.add_argument("--partition", "-p", help="Partition number", type=int)
    parser.add_argument('--key', '-k', help='Key')
    parser.add_argument('--filter', '-f', help='comma-separated list of '
                                               'filters in format key=value')
    timestamp_group_desc = """Arguments for timestamp filtering. Use
                           --timestamp to filter for an exact match and
                           --start/--end to filter for a range. Exact and range
                           filtering are exclusive. Range filtering accepts
                           either one or both arguments. Timestamps can be
                           specified as UNIX epoch in (milli)seconds or in the
                           format '%Y-%m-%dT%H:%M'."""
    timestamp_group = parser.add_argument_group('Timestamps', timestamp_group_desc)
    timestamp_group.add_argument('--timestamp', '-ts', help='Timestamp for '
                                                            'exact-match '
                                                            'filtering')
    timestamp_group.add_argument('--start', '-st', help='Start timestamp for '
                                                        'range filtering')
    timestamp_group.add_argument('--end', '-e', help='End timestamp for range '
                                                     'filtering')


    args = parser.parse_args()

    exact_ts_arg = args.timestamp
    start_ts_arg = args.start
    end_ts_arg = args.end
    if exact_ts_arg and (start_ts_arg or end_ts_arg):
        print('Error: --timestamp argument is exclusive with --start/--end',
              file=sys.stderr)
        exit(1)
    if exact_ts_arg:
        timestamp = parse_timestamp(exact_ts_arg)
        start_ts = timestamp  # Set this to start reading at the correct offset.
        print('Filtering for timestamp:', timestamp,
              datetime.utcfromtimestamp(timestamp / 1000)
              .strftime('%Y-%m-%dT%H:%M'))
    if start_ts_arg:
        start_ts = parse_timestamp(start_ts_arg)
        print('Starting read at timestamp:', start_ts,
              datetime.utcfromtimestamp(start_ts / 1000)
              .strftime('%Y-%m-%dT%H:%M'))
    if end_ts_arg:
        end_ts = parse_timestamp(end_ts_arg)
        print('Ending read at timestamp:', end_ts,
              datetime.utcfromtimestamp(end_ts / 1000)
              .strftime('%Y-%m-%dT%H:%M'))
    key = args.key
    if key:
        print('Filtering for key:', key)
    partition = args.partition
    if partition:
        print('Forcing partition number:', partition)
    filter_list = args.filter
    filter_dict = dict()
    if filter_list:
        for filter_ in filter_list.split(','):
            key_value_pair = filter_.split('=')
            if len(key_value_pair) != 2:
                print('Error: Invalid filter format:', filter_, file=sys.stderr)
                exit(1)
            filter_dict[key_value_pair[0]] = key_value_pair[1]
    if filter_dict:
        print('Applying filter:', filter_dict)


    c = Consumer({
        'bootstrap.servers': args.server,
        'group.id': 'ihr_grep',
        'auto.offset.reset': 'earliest',
        })
    try:
        if partition:
            on_assign(c, [TopicPartition(args.topic, partition=partition)])
        else:
            c.subscribe([args.topic], on_assign=on_assign)
        partition_paused = 0

        nb_read = 0
        while True:
            msg = c.poll(10)

            if msg is None:
                break

            if msg.error():
                print("Consumer error: {}".format(msg.error()))
                continue
            if key is not None and msg.key() != key:
                continue
            msg_ts = msg.timestamp()
            if timestamp\
                    and msg_ts[0] == confluent_kafka.TIMESTAMP_CREATE_TIME\
                    and msg_ts[1] != timestamp:
                continue
            if end_ts != confluent_kafka.OFFSET_END\
                    and msg_ts[0] == confluent_kafka.TIMESTAMP_CREATE_TIME\
                    and msg_ts[1] >= end_ts:
                c.pause([TopicPartition(msg.topic(), msg.partition())])
                partition_paused += 1
                if partition_paused < partition_total:
                    continue
                break
            data = msgpack.unpackb(msg.value(), raw=False)
            filtered = False
            for filter_key in filter_dict:
                if filter_key in data\
                        and str(data[filter_key]) != filter_dict[filter_key]:
                    filtered = True
                    break
            if filtered:
                continue
            nb_read += 1
            print(data)
            if args.num_msg is not None and nb_read >= args.num_msg:
                break
    finally:
        c.close()
