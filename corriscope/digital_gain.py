"""HDF5 archive of the F-engine digital (scaler) gains.

Each *update* stores, for every correlator input, the per-frequency-bin gain
coefficient and the common exponent that were programmed into the scalers, with the
time the gains were computed and a unique ``update_id`` string. Files live in::

    {output_dir}/{YYYYMMDDTHHMMSSZ}_{instrument_name}_{output_suffix}/{seconds_since_start:08d}.h5

and share the layout of the archives pychfpga wrote with ``wtl.h5-archive``: a group
``index_map`` holding the axes (``update_time``, ``freq``, ``input``) and one dataset per
entry of :attr:`DigitalGainArchive._dataset_spec`, each tagged with an ``axis`` attribute.
Inputs are keyed by a correlator-input serial string so a gain table survives a board
moving to another slot.
"""
import datetime
import glob
import logging
import os
import time
from calendar import timegm

import h5py
import numpy as np

__version__ = '0.6'
__archive_version__ = '3.2.0'

_STR = h5py.special_dtype(vlen=str)


class DigitalGainArchive:
    """Read/write access to a directory of digital-gain HDF5 files."""

    _uniq_id = 'update_id'
    _grow_ax = 'update_time'

    _axes = {
        'update_time': {'dtype': np.float64},
        'freq': {'dtype': np.dtype([('centre', '<f8'), ('width', '<f8')])},
        'input': {'dtype': np.dtype([('chan_id', 'u2'), ('correlator_input', 'U32')])},
    }

    _dataset_spec = {
        'update_id': {'axes': ['update_time'], 'dtype': _STR},
        'compute_time': {'axes': ['update_time', 'input'], 'dtype': np.float64},
        'gain_coeff': {'axes': ['update_time', 'freq', 'input'], 'dtype': np.complex64},
        'gain_exp': {'axes': ['update_time', 'input'], 'dtype': np.int32},
    }

    def __init__(self, output_dir=None, output_suffix='digitalgain', instrument_name='crs',
                 notes='', search=True, max_num=1, max_file_size=1e9, attrs=None, **axes):
        """
        Parameters:
            output_dir (str): directory holding the acquisition directories.
            output_suffix (str): suffix of the acquisition directory names.
            instrument_name (str): correlator name, used in directory names and file attrs.
            notes (str): free text saved in the file attributes.
            search (bool): open existing acquisitions in ``output_dir`` with the same axes
                for reading, so :meth:`read_gain` can return earlier updates.
            max_num (int): updates per file before a new file is started.
            max_file_size (float): bytes per file before a new file is started.
            attrs (dict): extra file attributes (e.g. ``git_version_tag``).
            **axes: the non-growing axes, ``freq=`` and ``input=`` (numpy arrays with
                the dtypes in :attr:`_axes`).
        """
        self.log = logging.getLogger(__name__)
        self.output_dir = output_dir or '.'
        self.output_suffix = output_suffix
        self.max_num = max_num if max_num is not None else float('inf')
        self.max_file_size = max_file_size
        self.attrs = {'instrument_name': instrument_name, 'version': __version__,
                      'archive_version': __archive_version__, 'notes': notes}
        self.attrs.update(attrs or {})

        self.axes = {}
        for ax, spec in self._axes.items():
            if ax == self._grow_ax:
                continue
            if ax not in axes:
                raise ValueError(f'Must pass the axis {ax!r} as a keyword argument')
            self.axes[ax] = np.asarray(axes[ax]).astype(spec['dtype'])

        self._files = []          # all archive files, oldest first
        self._lookup = {}         # update_id -> (filename, row, update_time)
        self.current_file = None  # file currently written to
        self._num = 0             # rows in current file
        self.last_read_file = None
        self.log.info(f'{self!r}: digital gain output directory is {self.output_dir}')

        if search:
            pattern = os.path.join(self.output_dir, f'*{instrument_name}_{output_suffix}', '*.h5')
            for path in sorted(glob.glob(pattern)):
                if self._axes_match(path):
                    self._register(path)
            self.log.info(f'{self!r}: found {len(self._files)} compatible gain file(s)')

        self.buffer = {}
        self._init_buffer()
        if self.last_update_id is not None:
            for dset in self.buffer:
                self.buffer[dset] = np.array(self.read(self.last_update_id, dset))

    def __repr__(self):
        return f'{type(self).__name__}({self.output_dir!r})'

    # ------------------------------------------------------------------ file handling
    def _init_buffer(self):
        for dset, spec in self._dataset_spec.items():
            if dset == self._uniq_id:
                continue
            shape = [self.axes[ax].size for ax in spec['axes'] if ax != self._grow_ax]
            self.buffer[dset] = np.zeros(shape, dtype=spec['dtype'])

    def _axes_match(self, path):
        try:
            with h5py.File(path, 'r') as hf:
                for ax, val in self.axes.items():
                    stored = hf['index_map'][ax][:]
                    if stored.size != val.size or not np.all(_as_unicode(stored, val.dtype) == val):
                        return False
            return True
        except (OSError, KeyError):
            self.log.warning(f'{self!r}: ignoring unreadable gain file {path}')
            return False

    def _register(self, path):
        with h5py.File(path, 'r') as hf:
            times = hf['index_map'][self._grow_ax][:]
            uids = hf[self._uniq_id][:]
        for row, (uid, t) in enumerate(zip(uids, times)):
            uid = uid.decode() if isinstance(uid, bytes) else str(uid)
            self._lookup[uid] = (path, row, float(t))
        if path not in self._files:
            self._files.append(path)

    def get_output_file(self, smp, run_name=None, **_):
        """Path of the file for an update at unix time ``smp``.

        The acquisition directory is named after the run start (``run_name[:16]`` when the
        fpga_master run name is given, otherwise ``smp``); the file after the seconds since.
        """
        prefix = run_name[:16] if run_name else datetime.datetime.fromtimestamp(smp, datetime.timezone.utc).strftime('%Y%m%dT%H%M%SZ')
        start = timegm(datetime.datetime.strptime(prefix, '%Y%m%dT%H%M%SZ').timetuple())
        folder = os.path.join(self.output_dir, f'{prefix}_{self.attrs["instrument_name"]}_{self.output_suffix}')
        os.makedirs(folder, exist_ok=True)
        return os.path.join(folder, f'{int(smp - start):08d}.h5')

    def change_file(self, smp, run_name=None, **_):
        """True when the run name no longer matches the acquisition being written."""
        if not self.current_file or not run_name:
            return False
        acq = os.path.basename(os.path.dirname(self.current_file))
        return run_name[:16] != acq[:16]

    def _create_file(self, path):
        with h5py.File(path, 'w') as hf:
            hf.attrs['acquisition_name'] = os.path.basename(os.path.dirname(path))
            for k, v in self.attrs.items():
                hf.attrs[k] = v
            im = hf.create_group('index_map')
            im.create_dataset(self._grow_ax, (0,), maxshape=(None,), dtype=self._axes[self._grow_ax]['dtype'])
            for ax, val in self.axes.items():
                im.create_dataset(ax, data=_as_bytes(val))
            for name, spec in self._dataset_spec.items():
                shape = tuple(0 if ax == self._grow_ax else self.axes[ax].size for ax in spec['axes'])
                maxshape = tuple(None if ax == self._grow_ax else n for ax, n in zip(spec['axes'], shape))
                d = hf.create_dataset(name, shape, maxshape=maxshape, dtype=spec['dtype'])
                d.attrs['axis'] = np.array(spec['axes'], dtype=_STR)
        self.current_file = path
        self._num = 0
        if path not in self._files:
            self._files.append(path)

    # ------------------------------------------------------------------ public API
    @property
    def archive_files(self):
        return list(self._files)

    @property
    def last_update(self):
        """Unix time of the most recent update (None if the archive is empty)."""
        return max((t for _, _, t in self._lookup.values()), default=None)

    @property
    def last_update_id(self):
        if not self._lookup:
            return None
        return max(self._lookup, key=lambda uid: self._lookup[uid][2])

    def _locate(self, key):
        """Resolve an update_id or a unix time (most recent update at or before it)."""
        if key is None:
            key = self.last_update_id
        if key in self._lookup:
            return self._lookup[key]
        if isinstance(key, (int, float, np.integer, np.floating)):
            before = [(t, uid) for uid, (_, _, t) in self._lookup.items() if t <= key]
            if before:
                return self._lookup[max(before)[1]]
        raise KeyError(f'No gain update matching {key!r}')

    def read(self, key, dataset):
        """Return ``dataset`` for the update selected by ``key`` (update_id or unix time)."""
        path, row, _ = self._locate(key)
        with h5py.File(path, 'r') as hf:
            data = hf[dataset][row]
        self.last_read_file = path
        if dataset == self._uniq_id:
            return data.decode() if isinstance(data, bytes) else str(data)
        return data

    def write(self, smp=None, **kwargs):
        """Append the buffer (see :meth:`set_gain`) as a new update at unix time ``smp``.

        ``run_name`` (keyword) names the fpga_master run and selects the acquisition
        directory. ``update_id`` may be given; the default is ``{suffix}_{UTC time}``.
        """
        smp = time.time() if smp is None else smp
        uid = kwargs.get('update_id') or '_'.join(
            [self.output_suffix, datetime.datetime.fromtimestamp(smp, datetime.timezone.utc).strftime('%Y%m%dT%H%M%S.%fZ')])
        if uid in self._lookup:
            raise ValueError(f'{self._uniq_id} = {uid} already exists in the archive')
        if self.last_update is not None and smp < self.last_update:
            raise ValueError(f'{smp:.2f} is before the last update at {self.last_update:.2f}')

        too_big = self.current_file and os.path.getsize(self.current_file) >= self.max_file_size
        if (self.current_file is None or self._num >= self.max_num or too_big
                or self.change_file(smp, **kwargs)):
            self._create_file(self.get_output_file(smp, **kwargs))

        with h5py.File(self.current_file, 'r+') as hf:
            row = self._num
            hf['index_map'][self._grow_ax].resize((row + 1,))
            hf['index_map'][self._grow_ax][row] = smp
            for name in self._dataset_spec:
                d = hf[name]
                d.resize(row + 1, axis=0)
                d[row] = uid if name == self._uniq_id else self.buffer[name]
        self._num += 1
        self._lookup[uid] = (self.current_file, row, float(smp))
        return uid

    def set_gain(self, gain, compute_time=None):
        """Update the buffer: ``gain`` is ``{input_serial: (gain_coeff_vector, gain_exp)}``.

        ``compute_time`` is a unix time or ``{input_serial: unix_time}`` (default: now).
        """
        now = time.time() if compute_time is None else compute_time
        for sn, (gcoeff, gexp) in gain.items():
            idx = self.chan_id[sn]
            self.buffer['gain_coeff'][:, idx] = gcoeff
            self.buffer['gain_exp'][idx] = gexp
            self.buffer['compute_time'][idx] = now[sn] if isinstance(now, dict) else now

    def read_gain(self, update_id=None):
        """Return ``(gain, compute_time)`` dicts keyed by input serial for an update.

        ``update_id`` may be an id string, a unix time (latest update before it) or None
        (latest update). ``gain[sn]`` is ``[gain_coeff_vector, gain_exp]``.
        """
        gain_coeff = self.read(update_id, 'gain_coeff')
        gain_exp = self.read(update_id, 'gain_exp')
        gain_time = self.read(update_id, 'compute_time')
        gain, compute_time = {}, {}
        for i, inp in enumerate(self.axes['input']['correlator_input']):
            gain[str(inp)] = [gain_coeff[:, i], gain_exp[i]]
            compute_time[str(inp)] = gain_time[i]
        return gain, compute_time

    def get_current_gain_file(self):
        """Path of the file the most recent update was written to (None if none yet)."""
        return self.current_file

    def close_all(self):
        """Files are opened per call, so there is nothing to close; kept for callers."""
        self.current_file = None
        self._num = 0

    @property
    def chan_id(self):
        """``{correlator_input_serial: index into the input axis}``."""
        try:
            return self._chan_id
        except AttributeError:
            self._chan_id = {str(inp['correlator_input']): int(inp['chan_id']) for inp in self.axes['input']}
            return self._chan_id


def _as_bytes(arr):
    """HDF5 cannot store numpy unicode: convert ``U`` fields of a structured array to ``S``."""
    if arr.dtype.names is None:
        return arr.astype(f'S{arr.dtype.itemsize // 4}') if arr.dtype.kind == 'U' else arr
    new_dtype = np.dtype([(n, f'S{arr.dtype[n].itemsize // 4}' if arr.dtype[n].kind == 'U' else arr.dtype[n])
                          for n in arr.dtype.names])
    out = np.empty(arr.shape, dtype=new_dtype)
    for n in arr.dtype.names:
        out[n] = arr[n].astype(new_dtype[n]) if arr.dtype[n].kind == 'U' else arr[n]
    return out


def _as_unicode(arr, dtype):
    """Inverse of :func:`_as_bytes` for comparison with the in-memory axis."""
    if arr.dtype.names is None:
        return arr.astype(dtype)
    out = np.empty(arr.shape, dtype=dtype)
    for n in dtype.names:
        out[n] = arr[n].astype(dtype[n])
    return out
