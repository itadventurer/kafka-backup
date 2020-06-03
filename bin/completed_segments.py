#!/usr/bin/env python
"""
Find (and delete) kafka-backup segment files
"""

import os
import re
import sys
import argparse


def filename_pattern():
    """ Kafka-backup segment/index filename pattern.

    Using function as constant string.
    This pattern is used to make filename-matching regex below too.
    """
    return 'segment_partition_%s_from_offset_%s_%s'


def oneplus(string):
    """ Check argument is >= 1 """
    value = int(string)
    if value < 1:
        raise argparse.ArgumentTypeError("cannot be less than 1")
    return value


def parse_args():
    """ Parse cmdline args """
    parser = argparse.ArgumentParser(
        description='Find (and delete) kafka-backup segment files'
    )
    parser.add_argument(
        '-d', '--delete',
        help='delete segment files',
        action='store_true',
        default=False,
    )
    parser.add_argument(
        '-k', '--keep',
        help='keep N segment files (one by default)',
        type=oneplus,
        default=1,
        metavar='N',
    )
    parser.add_argument(
        '-l', '--list',
        help='list segment files',
        action='store_true',
        default=False,
    )
    parser.add_argument(
        'target_dir',
        help='backup directory (target.dir)',
        default=os.getcwd(),
    )
    return parser.parse_args()


def collect_segments(target_dir):
    """ Collect segment partitions & offsets per directory (topic)

    Args:
        target_dir (str): Kafka-backup target.dir

    Returns:
        A dict mapping keys to topics. Each value is dict of partitions with
        list of offsets in it. For example

        topic1: {
            000: [ 0000000000 ],
        },
        topic2: {
            000: [ 0000000021 ],
            001: [ 0000000391 ],
            002: [ 0000001291, 0000018423 ]
        },
        ...

    """

    # Make regex from pattern
    # Implemented this way to keep file pattern just in single place (filename_pattern() above)
    fregex = re.compile(r"^%s$" % (filename_pattern() % (r'(\d{3})', r'(\d{10})', 'records')))
    res = {}
    # Traverse dirtree to collect offsets in partitions per topic
    for tdir, _, files in os.walk(target_dir):
        if tdir == target_dir:
            continue
        res[tdir] = {}
        for segfile in sorted(files):
            match = fregex.match(segfile)
            if match:
                (partition, offset) = match.groups()
                if partition not in res[tdir]:
                    res[tdir][partition] = []
                res[tdir][partition].append(offset)
    return res


def process_segment(tdir, partition, offset, do_delete, do_list):
    """ Process segment

    Args:
        tdir (str): topic directory
        partition (str): topic partition
        offset (str): starting segment offset
        do_delete (bool): delete segment files?
        do_list (boot): list segment files?
    """
    index_file = filename_pattern() % (partition, offset, 'index')
    records_file = filename_pattern() % (partition, offset, 'records')

    if do_delete or do_list:
        index_path = os.path.join(tdir, index_file)
        records_path = os.path.join(tdir, records_file)
        if do_list:
            print(index_path)
            print(records_path)
        if do_delete:
            os.unlink(index_path)
            os.unlink(records_path)
    else:
        print("Topic %s, First offset %s - Index file: %s Records File: %s"
              % (os.path.basename(tdir), offset, index_file, records_file))


def main():
    """ int main(int argc, char **argv) """
    args = parse_args()

    segs = collect_segments(args.target_dir)

    for tdir, seg_data in segs.items():
        for partition, offsets in seg_data.items():
            for i, offset in enumerate(offsets):
                # Perform action requested (list/delete/default) on the file
                # Skip (keep) last `args.keep` files (1 by default)
                # That one skipped by default file is usually incompleted so must be kept anyway
                if i < len(offsets) - args.keep:
                    process_segment(tdir, partition, offset, args.delete, args.list)
    return 0


if __name__ == '__main__':
    sys.exit(main())
