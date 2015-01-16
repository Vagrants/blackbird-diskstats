"""
blackbird diskstats plugin

send data to backend "/proc/diskstats" information
"""

__VERSION__ = '0.1.0'

import re
from blackbird.plugins import base

DISKSTATS_TABLE = [
    'major', 'minor', 'disk_name',
    'read_ios', 'read_merges', 'read_sectors', 'read_ticks',
    'write_ios', 'write_merges', 'write_sectors', 'write_ticks',
    'in_flight', 'io_ticks', 'time_in_queue',
]


class ConcreteJob(base.JobBase):
    """
    This class is Called by "Executor".
    """

    def __init__(self, options, queue=None, logger=None):
        super(ConcreteJob, self).__init__(options, queue, logger)

    def build_items(self):
        """
        main loop
        """

        # ping item
        self._ping()

        # get /proc/diskstats
        self._diskstats()

    def build_discovery_items(self):
        """
        main loop for lld
        """

        # discovery disk names
        self._lld_disk_names()

    def _enqueue(self, key, value, prefix=True):
        """
        wrap queue method
        """

        if isinstance(key, list):
            key = ','.join(str(_key) for _key in key)

        if prefix:
            key = 'diskstats[{0}]'.format(key)

        item = DiskStatsItem(
            key=key,
            value=value,
            host=self.options['hostname']
        )
        self.queue.put(item, block=False)
        self.logger.debug(
            'Inserted to queue {key}:{value}'
            ''.format(key=key, value=value)
        )

    def _enqueue_lld(self, key, value):
        """
        enqueue lld item
        """

        item = base.DiscoveryItem(
            key=key,
            value=value,
            host=self.options['hostname']
        )
        self.queue.put(item, block=False)
        self.logger.debug(
            'Inserted to lld queue {key}:{value}'
            ''.format(key=key, value=str(value))
        )

    def _ping(self):
        """
        send ping item
        """

        self._enqueue('blackbird.diskstats.ping', 1, prefix=False)
        self._enqueue('blackbird.diskstats.version', __VERSION__, prefix=False)

    @staticmethod
    def _open_diskstats():
        """
        open "/proc/diskstats" and return dict.
        """

        _ret = dict()

        try:
            with open('/proc/diskstats') as _diskstats:
                for _line in _diskstats.readlines():
                    _ds = _line.split()

                    # skip ramN and loopN
                    if re.match(r'^(ram|loop)[0-9]+$', _ds[2]):
                        continue

                    _ret[_ds[2]] = dict()
                    for _key, _value in zip(DISKSTATS_TABLE[3:], _ds[3:]):
                        _ret[_ds[2]][_key] = _value

        except IOError:
            raise base.BlackbirdPluginError(
                'can not open /proc/diskstats'
            )

        return _ret

    def _diskstats(self):
        """
        send diskstats
        """

        _ds = self._open_diskstats()
        for _disk_name in _ds.keys():
            for _key, _value in _ds[_disk_name].items():
                self._enqueue(
                    [_disk_name, _key],
                    int(_value),
                )

    def _lld_disk_names(self):
        """
        discovery disk names
        """

        _ds = self._open_diskstats()
        self._enqueue_lld(
            'diskstats.disknames.LLD',
            [{'{#DISKNAME}': _dn} for _dn in _ds.keys()]
        )


# pylint: disable=too-few-public-methods
class DiskStatsItem(base.ItemBase):
    """
    Enqued item.
    """

    def __init__(self, key, value, host):
        super(DiskStatsItem, self).__init__(key, value, host)

        self._data = {}
        self._generate()

    @property
    def data(self):
        return self._data

    def _generate(self):
        self._data['key'] = self.key
        self._data['value'] = self.value
        self._data['host'] = self.host
        self._data['clock'] = self.clock


class Validator(base.ValidatorBase):
    """
    Validate configuration.
    """

    def __init__(self):
        self.__spec = None

    @property
    def spec(self):
        self.__spec = (
            "[{0}]".format(__name__),
            "hostname=string(default={0})".format(self.detect_hostname()),
        )
        return self.__spec
