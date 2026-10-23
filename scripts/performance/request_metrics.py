"""Streaming attempt accounting from the latency proxy's monotonic evidence."""
import collections
import json
import math


class Requests:
    def __init__(self, interval=None):
        self.interval = interval
        self.methods, self.classes, self.statuses = (collections.Counter() for _ in range(3))
        self.uploaded = self.downloaded = self.unknown_sizes = self.errors = 0

    def add(self, row):
        if 'error' in row:
            self.errors += 1
        if 'ordinal' not in row:
            return
        method = row['method']
        category = ('class_a' if method == 'PUT' or (method == 'GET' and row['listing']) else
                    'class_b' if method in {'GET', 'HEAD'} else
                    'free' if method == 'DELETE' else 'unclassified')
        self.methods[method] += 1
        self.classes[category] += 1
        self.statuses[str(row['status'])] += 1
        if row['request_body_bytes'] is None:
            self.unknown_sizes += 1
        else:
            self.uploaded += row['request_body_bytes']
        self.downloaded += row['response_body_bytes']

    def report(self):
        return {'interval': self.interval, 'attempts': sum(self.methods.values()),
                'methods': dict(self.methods), 'estimated_r2_classes': dict(self.classes),
                'status_codes': dict(self.statuses), 'transport_errors': self.errors,
                'request_body_bytes': self.uploaded, 'response_body_bytes': self.downloaded,
                'unknown_request_body_sizes': self.unknown_sizes,
                'scope': ('Completed proxy attempts in the specified monotonic interval. ' if self.interval else
                          'All completed proxy attempts, including bootstrap, drain, GC and retries. ') +
                         'Class estimates use HTTP method/listing flags; POST is unclassified because '
                         'multipart versus bulk deletion is not recorded. Not a billing invoice.'}


def rows(directory):
    with (directory/'proxy.jsonl').open() as stream:
        for line in stream:
            row = json.loads(line)
            if 'ordinal' in row or 'error' in row:
                yield row


def object_requests(directory, interval=None):
    counts = Requests(interval)
    for row in rows(directory):
        if interval is None or interval[0] <= row['completed_monotonic'] < interval[1]:
            counts.add(row)
    return counts.report()


def request_windows(directory, start, duration, seconds):
    """One pass regardless of experiment length; boundary requests belong to the next bin."""
    bins = [Requests((start+offset, start+min(offset+seconds, duration)))
            for offset in range(0, duration, seconds)]
    for row in rows(directory):
        at = row['completed_monotonic']
        if start <= at < start+duration:
            bins[math.floor((at-start)/seconds)].add(row)
    return [counts.report() for counts in bins]
