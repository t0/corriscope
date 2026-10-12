"""Checks for the in-tree replacements of the wtl.* helpers and the gain archive."""
import asyncio
import logging

import numpy as np
import pytest

from corriscope.common import log
from corriscope.common.config import load_yaml_config
from corriscope.common.metrics import Metrics
from corriscope.common.namespace import NameSpace, merge_dict
from corriscope.common.rest import AsyncRESTClient, AsyncRESTServer, endpoint
from corriscope.digital_gain import DigitalGainArchive


def test_namespace_nested_access_and_as_dict():
    ns = NameSpace(a=1, b=dict(c=2, d=[1, {'e': 3}]))
    assert ns.a == 1 and ns.b.c == 2 and ns['b']['c'] == 2
    assert isinstance(ns.b, NameSpace)
    ns.b.c = 5
    ns['f'] = 6
    assert ns.as_dict() == {'a': 1, 'b': {'c': 5, 'd': [1, {'e': 3}]}, 'f': 6}
    with pytest.raises(KeyError):
        ns.missing
    assert ns.get('missing', 7) == 7 and 'a' in ns and len(ns) == 3
    assert dict(ns.items())['a'] == 1
    assert NameSpace(ns).as_dict() == ns.as_dict() and ns.deepcopy().as_dict() == ns.as_dict()
    assert 'x: 1' in NameSpace(x=1).as_yaml()


def test_merge_dict():
    src = {'a': 1, 'b': {'c': 2, 'd': 3}, 'l': [1]}
    dest = {'b': {'c': 9}, 'e': 4, 'l': [2]}
    assert merge_dict(src, dest) == {'a': 1, 'b': {'c': 9, 'd': 3}, 'e': 4, 'l': [2]}
    assert merge_dict(src, dest, add_lists=True)['l'] == [1, 2]
    assert merge_dict(src, None, skip_none=True) is src
    with pytest.raises(TypeError):
        merge_dict({'a': 1}, 5)
    assert NameSpace(src).merge(dest).as_dict()['e'] == 4


def test_load_yaml_config(tmp_path):
    f = tmp_path / 'c.yaml'
    f.write_text('obj1:\n  f11: 11\n  obj11:\n    f111: 111\n  obj12:\n    f121: 121\nobj2:\n  f21: 21\n')
    assert load_yaml_config(None) == {}
    assert load_yaml_config(f'{f}:obj1') == {'f11': 11, 'obj11': {'f111': 111}, 'obj12': {'f121': 121}}
    assert load_yaml_config(f'{f}:obj1.obj11 .obj12') == {'f111': 111, 'f121': 121}
    assert load_yaml_config(f'{f}:obj1 obj2 f11=5 obj11.f111=[1,2]') == {
        'f11': 5, 'obj11': {'f111': [1, 2]}, 'obj12': {'f121': 121}, 'f21': 21}
    with pytest.raises(AttributeError):
        load_yaml_config(f'{f}:nope')


def test_metrics_text_format_and_merge():
    m = Metrics(type='GAUGE', slot=3)
    m.add('a', value=1.5, time=1000, chan=0)
    m.add('a', value=2, time=2000, chan=1, doc='doc a', type='gauge')
    other = Metrics(default_type='counter')
    other.add('b', value=7, time=3000)
    m += other
    text = m.get_str()
    assert '# HELP a doc a' in text and '# TYPE a gauge' in text and '# TYPE b counter' in text
    # merged samples pick up the receiving object's default labels, as the originals did
    assert 'a{chan="0",slot="3",type="GAUGE"} 1.5 1000' in text and 'b{slot="3",type="GAUGE"} 7 3000' in text
    late = m.get_str(after=2500)   # headers are always emitted; only samples are time-filtered
    assert len(m) == 3 and 'b{slot="3",type="GAUGE"} 7 3000' in late and ' 1.5 1000' not in late and ' 2 2000' not in late
    popped = m.pop()
    assert len(m) == 0 and len(popped) == 3 and popped.get_gzip()
    latest = Metrics(latest_only=True)
    latest.add('x', value=1, time=1); latest.add('x', value=2, time=2)
    assert len(latest) == 1


def test_log_helpers():
    class Foo:
        pass
    assert log.get_logger(Foo()).name == f'{__name__}.Foo'
    assert log.get_logger('a', 'b').name == 'a.b'
    cfg = {'formatters': {'f': {'format': '%(message)s'}},
           'handlers': {'h': {'class': 'logging.StreamHandler', 'formatter': 'f', 'level': 'debug'}},
           'loggers': {'pkg.mod': {'handlers': ['h'], 'level': 'info'}}}
    new = log.setup_logging(cfg, log_levels={'h': 'WARNING'}, base_package_name='pkg',
                            actual_package_name='corriscope_test', script_name='pkg.mod')
    assert set(new) == {'corriscope_test.mod', 'corriscope_test.__main__'}
    assert new['corriscope_test.mod'][0].level == logging.WARNING
    log.stop_logging(new)
    assert not logging.getLogger('corriscope_test.mod').handlers


def test_rest_roundtrip():
    class S(AsyncRESTServer):
        DEFAULT_PORT = 0

        @endpoint('ping')
        async def ping(self):
            return {'pong': True}

        @endpoint('add')
        async def add(self, x, y=1):
            return {'sum': x + y}

        @endpoint('boom')
        async def boom(self):
            raise ValueError('bad')

    async def main():
        import socket
        with socket.socket() as s:
            s.bind(('127.0.0.1', 0))
            port = s.getsockname()[1]
        server = S('127.0.0.1', port)
        await asyncio.sleep(0.2)
        client = AsyncRESTClient('127.0.0.1', port)
        try:
            assert await client.get('ping') == {'pong': True}
            assert await client.post('add', x=2, y=3) == {'sum': 5}
            assert await client.post('add', x=2) == {'sum': 3}
            with pytest.raises(RuntimeError, match='bad'):
                await client.get('boom')
        finally:
            await client.close()
            await server.close()
    asyncio.run(main())


def test_digital_gain_archive_roundtrip(tmp_path):
    nfreq, ninp = 8, 3
    freq = np.array(list(zip(np.arange(nfreq) * 1.0, [1.0] * nfreq)), dtype=DigitalGainArchive._axes['freq']['dtype'])
    inputs = np.array([(i, f'crs00{i}') for i in range(ninp)], dtype=DigitalGainArchive._axes['input']['dtype'])
    arch = DigitalGainArchive(output_dir=str(tmp_path), instrument_name='corr8', freq=freq, input=inputs,
                              attrs={'git_version_tag': 'test'})
    assert arch.last_update is None
    g = {f'crs00{i}': (np.full(nfreq, 10.0 * (i + 1), dtype=complex), i) for i in range(ninp)}
    arch.set_gain(g, compute_time=1000.0)
    uid1 = arch.write(smp=2000.0, run_name='20260101T000000Z_corr8')
    arch.set_gain({'crs001': (np.ones(nfreq), 5)}, compute_time=2500.0)
    uid2 = arch.write(smp=3000.0, run_name='20260101T000000Z_corr8')
    assert arch.last_update == 3000.0 and arch.last_update_id == uid2
    assert arch.read(2999.0, 'update_id') == uid1          # unix time -> latest update before it
    gains, times = arch.read_gain(uid1)
    assert gains['crs002'][1] == 2 and np.allclose(gains['crs002'][0], 30) and times['crs000'] == 1000.0
    gains2, _ = arch.read_gain()                            # latest
    assert gains2['crs001'][1] == 5 and np.allclose(gains2['crs001'][0], 1)
    assert len(arch.archive_files) == 2                     # max_num=1 -> one update per file
    with pytest.raises(ValueError):
        arch.write(smp=10.0)

    reopened = DigitalGainArchive(output_dir=str(tmp_path), instrument_name='corr8', freq=freq, input=inputs)
    assert reopened.last_update_id == uid2 and len(reopened.archive_files) == 2
    assert reopened.read_gain(uid1)[0]['crs000'][1] == 0
    assert np.allclose(reopened.buffer['gain_exp'], [0, 5, 2])
