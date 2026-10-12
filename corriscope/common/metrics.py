"""Minimal container for Prometheus-style metrics (replaces the private ``wtl.metrics``).

A ``Metrics`` object holds ``{metric_name: {'type', 'doc', 'entries'}}`` where ``entries``
maps a frozenset of ``(label, value)`` pairs to ``{time_ms: value}``. Metrics objects can be
added together (``m += other``), rendered in the Prometheus text exposition format
(``str(m)`` / ``get_str``) or gzip-compressed (``get_gzip``).
"""
import gzip
import logging
import time as _time


class Metrics:
    def __init__(self, arg=None, default_type=None, latest_only=False, **default_labels):
        """
        Parameters:
            arg (Metrics, dict or list): initial content, merged with :meth:`add`.
            default_type (str): type used for metrics added without an explicit ``type``.
            latest_only (bool): keep only the most recent sample per label set.
            **default_labels: labels attached to every sample added to this object.
        """
        self.default_type = default_type
        self.default_labels = default_labels
        self.latest_only = latest_only
        self.last_time = 0
        self.last_time_table = {}
        self.last_compression_ratio = 0
        self.log = logging.getLogger(__name__)
        self.metrics = {}
        if isinstance(arg, dict):
            other = Metrics()
            other.metrics = dict(arg)
            self.add(other)
        elif arg is not None:
            self.add(arg)

    def items(self):
        return list(self.metrics.items())

    def as_dict(self):
        return self.metrics

    def __len__(self):
        return sum(len(samples) for m in self.metrics.values() for samples in m['entries'].values())

    def __iadd__(self, other):
        return self.add(other)

    def clear(self):
        self.last_time = 0
        self.metrics = {}

    def pop(self):
        """Return a new Metrics holding the current content and empty this one."""
        out = Metrics()
        out.metrics = self.metrics
        self.clear()
        return out

    def get_last_time(self):
        return self.last_time

    def add(self, metric_name, value=None, type=None, doc=None, time=None, **labels):
        """Add a sample, declare a metric, or merge another Metrics object.

        ``metric_name`` may be a ``Metrics`` (merged, with ``labels`` added to every sample),
        a list of names, or a name. With ``value`` None only ``type``/``doc`` are recorded.
        ``time`` is milliseconds since the epoch (default: now).
        """
        if metric_name is None:
            return self
        if isinstance(metric_name, Metrics):
            for name, m in metric_name.metrics.items():
                self.add(name, type=m['type'], doc=m['doc'])
                for label_set, samples in m['entries'].items():
                    merged = dict(label_set)
                    merged.update(labels)
                    for t, v in samples.items():
                        self.add(name, value=v, time=t, **merged)
            return self
        if not isinstance(metric_name, str):
            for name in metric_name:
                self.add(name, value=value, type=type, doc=doc, time=time, **labels)
            return self

        metric = self.metrics.setdefault(metric_name, dict(type=None, doc=None, entries={}))
        if doc:
            if metric['doc'] and metric['doc'] != doc:
                raise RuntimeError(f'Cannot assign different docs to metric {metric_name}')
            metric['doc'] = doc
        type = type or self.default_type
        if type:
            if metric['type'] and metric['type'] != type.lower():
                raise RuntimeError(f'Cannot assign different types to metric {metric_name}')
            metric['type'] = type.lower()
        if value is None:
            return self

        t = int(time or _time.time() * 1000)
        self.last_time = max(self.last_time, t)
        all_labels = dict(self.default_labels)
        all_labels.update(labels)
        key = frozenset((k, str(v)) for k, v in all_labels.items())
        samples = metric['entries'].setdefault(key, {})
        if self.latest_only:
            samples.clear()
        elif t in samples:
            self.log.warning(f'{self!r}: metric {metric_name} at time {t} already exists; overwriting')
        samples[t] = value
        return self

    def get_str(self, after=0):
        """Prometheus text format. ``after`` is a time (ms) or a tag naming the previous call."""
        if isinstance(after, str):
            after_time = self.last_time_table.setdefault(after, 0)
        else:
            after_time = after or 0
        lines = []
        for name, m in self.metrics.items():
            if m['doc']:
                lines.append(f'# HELP {name} {m["doc"]}\n')
            if m['type']:
                lines.append(f'# TYPE {name} {m["type"]}\n')
            for label_set, samples in m['entries'].items():
                label_str = ('{' + ','.join(f'{k}="{v}"' for k, v in sorted(label_set)) + '}') if label_set else ''
                for t, v in sorted(samples.items()):
                    if t > after_time:
                        lines.append(f'{name}{label_str} {v:.16g} {t}\n')
        if isinstance(after, str):
            self.last_time_table[after] = self.last_time
        return ''.join(lines)

    __str__ = get_str

    def get_gzip(self, after=0):
        raw = self.get_str(after=after).encode()
        compressed = gzip.compress(raw)
        self.last_compression_ratio = 0 if not raw else 1 - len(compressed) / len(raw)
        return compressed
