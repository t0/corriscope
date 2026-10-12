"""Small aiohttp-based REST server/client base classes (replaces the private ``wtl.rest``).

Servers declare endpoints with the ``@endpoint('name')`` decorator on ``async`` methods.
A method with no parameters (besides ``self``) answers GET requests; one with parameters
answers POST requests whose JSON body supplies the keyword arguments. A parameter named
``request`` receives the aiohttp request. Return values are JSON-encoded unless they are
already an ``aiohttp.web.Response``; uncaught exceptions come back as ``{'error': text}``.

``AsyncRESTClient.get/post`` call those endpoints and raise ``RuntimeError`` on an error
reply. ``run_client`` is the shared command-line driver used by ``fpga_master``,
``raw_acq`` and ``gps``. ``SocketContext`` is a reconnecting TCP socket helper used for
instruments behind serial-to-ethernet adapters.
"""
import argparse
import asyncio
import inspect
import logging
import socket
import traceback

import aiohttp
import aiohttp.web
import yaml
from aiohttp import ClientError  # noqa: F401  (re-exported for callers)

from . import log
from .config import load_yaml_config


class AsyncMixin:
    """Periodic-task helpers shared by servers and clients."""

    def add_periodic_callback(self, callback, period, stop_on_errors=False):
        """Run ``await callback()`` every ``period`` milliseconds in the current loop."""
        async def periodic():
            while True:
                try:
                    await callback()
                except Exception as e:  # noqa: BLE001
                    self.log.error(f'{self!r}: unhandled exception in periodic callback {callback!r}: '
                                   f'{e!r}\n{traceback.format_exc()}')
                    if stop_on_errors:
                        raise
                await asyncio.sleep(period / 1000)
        asyncio.get_event_loop().create_task(periodic())

    def call_later(self, delay, callback):
        asyncio.get_event_loop().call_later(delay, callback)

    def add_heartbeat(self, heartbeat_string='.', period=1000):
        async def beat():
            self.log.info(f'{self!r}: *** Alive ***')
        self.add_periodic_callback(beat, period)

    async def shutdown(self):
        pass


def endpoint(endpoint_name):
    """Tag an ``async`` method as the handler of REST endpoint ``endpoint_name``."""
    if not isinstance(endpoint_name, str):
        raise TypeError(f'An endpoint name must be given to @endpoint, got {endpoint_name!r}')

    def wrapper(fn):
        spec = inspect.getfullargspec(fn)
        args = spec.args + (['**' + spec.varkw] if spec.varkw else [])
        fn.endpoint_info = (fn.__name__, endpoint_name, args)
        return fn
    return wrapper


class AsyncRESTServer(AsyncMixin):
    """aiohttp web server exposing the ``@endpoint`` methods of the subclass.

    The server task is created on the running event loop at construction time.
    """
    DEFAULT_PORT = 80

    def __init__(self, address='', port=DEFAULT_PORT, heartbeat_string=None, heartbeat_period=1):
        self.address = address
        self.port = port
        self.log = log.get_logger(self)
        self.log.info(f'{self!r}: Creating {type(self).__name__} server at {address or "*"}:{port}')
        self.app = aiohttp.web.Application()
        self.app.add_routes(self.get_endpoint_routes())
        self._runner = None

        async def run_server():
            self._runner = aiohttp.web.AppRunner(self.app)
            await self._runner.setup()
            await aiohttp.web.TCPSite(self._runner, address or None, port).start()
            self.log.debug(f'{self!r}: server started')
        asyncio.get_event_loop().create_task(run_server())

        if heartbeat_string:
            async def heartbeat():
                while True:
                    print(heartbeat_string)
                    await asyncio.sleep(heartbeat_period)
            asyncio.get_event_loop().create_task(heartbeat())

    def __repr__(self):
        return f'{type(self).__name__}({self.address}:{self.port})'

    async def close(self):
        if self._runner is not None:
            await self._runner.cleanup()

    def get_endpoint_routes(self):
        routes = []
        for attr in vars(type(self)).values():
            info = getattr(attr, 'endpoint_info', None)
            if not info:
                continue
            method_name, endpoint_name, method_args = info
            method = getattr(self, method_name)
            pass_request = 'request' in method_args
            is_post = len(method_args) > 1 + pass_request
            self.log.debug(f'{self!r}: REST {"POST" if is_post else "GET"} /{endpoint_name} '
                           f'-> {method_name}({", ".join(method_args)})')

            async def handler(request, _method=method, _is_post=is_post, _pass_request=pass_request):
                try:
                    body = await request.text()
                    kwargs = (await request.json() or {}) if _is_post and body else {}
                    if _pass_request:
                        kwargs['request'] = request
                    result = await _method(**kwargs)
                    if isinstance(result, aiohttp.web.Response):
                        return result
                    return aiohttp.web.json_response(result)
                except Exception as e:  # noqa: BLE001
                    msg = repr(e) + '\n\n' + traceback.format_exc()
                    self.log.error(msg)
                    return aiohttp.web.json_response(dict(error=msg))

            routes.append((aiohttp.web.post if is_post else aiohttp.web.get)('/' + endpoint_name, handler))
        return routes


class AsyncRESTClient(AsyncMixin):
    """HTTP client for an :class:`AsyncRESTServer`.

    If ``server_class`` is given and nothing answers at ``hostname:port``, a local server of
    that class is started and kept in ``self.server``.
    """
    DEFAULT_HOST = 'localhost'
    DEFAULT_PORT = 80

    def __init__(self, hostname=DEFAULT_HOST, port=DEFAULT_PORT, server_class=None,
                 heartbeat_string=None, heartbeat_period=1, connection_timeout=None,
                 request_timeout=None):
        self.log = log.get_logger(self)
        self.hostname = hostname
        self.port = port
        self.server = None
        if server_class and (not hostname or not self._tcp_ping(hostname, port)):
            self.log.info(f'{self!r}: hostname not given or not responding; creating a local server')
            self.hostname = 'localhost'
            self.server = server_class('', self.port)
        timeout = aiohttp.ClientTimeout(total=request_timeout, connect=connection_timeout)
        self.session = aiohttp.ClientSession(timeout=timeout)
        if heartbeat_string:
            async def heartbeat():
                while True:
                    print(heartbeat_string)
                    await asyncio.sleep(heartbeat_period)
            asyncio.get_event_loop().create_task(heartbeat())

    def __repr__(self):
        return f'{type(self).__name__}({self.hostname}:{self.port})'

    def url(self, endpoint):
        return f'http://{self.hostname}:{self.port}/{endpoint}'

    async def post(self, endpoint, *, raw=False, **kws):
        """POST ``kws`` as JSON to ``endpoint`` and return the decoded reply."""
        return await self._fetch(endpoint, 'POST', raw, **kws)

    async def get(self, endpoint, *, raw=False):
        return await self._fetch(endpoint, 'GET', raw)

    async def _fetch(self, endpoint, method, raw=False, **kws):
        url = self.url(endpoint)
        self.log.debug(f'{self!r}: {method} {url}')
        fetch = self.session.post if method == 'POST' else self.session.get
        async with fetch(url, json=kws, ssl=False) as resp:
            error = ''
            if raw:
                reply = await resp.text()
            else:
                try:
                    reply = await resp.json()
                    if isinstance(reply, dict):
                        error = reply.pop('error', '')
                except (TypeError, ValueError, aiohttp.ContentTypeError):
                    reply = {}
                    error = f'{self!r}: invalid JSON reply {await resp.text()!r}'
            if not resp.ok:
                error = f'{resp.status}:{resp.reason}\n{error}'
        if error:
            self.log.error(f'{self!r}: {method} {endpoint} failed: {error}')
            raise RuntimeError(f'Response={reply}, Error:{error}')
        return reply

    async def close(self):
        await self.session.close()

    def _tcp_ping(self, hostname, port, timeout=0.3):
        """True if a TCP connection to ``hostname:port`` succeeds within ``timeout``."""
        try:
            with socket.create_connection((hostname, port), timeout=timeout):
                return True
        except OSError:
            self.log.warning(f'Could not establish a TCP connection with {hostname}:{port}')
            return False


class SocketContext:
    """Re-entrant context manager owning one TCP socket to an instrument.

    ``with inst.socket(): inst.send(...); inst.recv()``. Nested contexts share the socket;
    it is closed when the outermost context exits (unless ``close_socket=False``).
    ``send``/``recv`` reconnect and retry up to three times on socket errors.
    """

    def __init__(self, hostname, port, timeout=0.5, flush_timeout=0.1, close_socket=True, **kwargs):
        self.log = log.get_logger(self)
        self.ip_addr = hostname
        self.ip_port = port
        self.timeout = timeout
        self.close_socket = close_socket
        self.flush_timeout = flush_timeout
        self.temp_flush_timeout = flush_timeout
        self._flush_on_enter = False
        self.sock = None
        self.socket_references = 0
        super().__init__(**kwargs)

    def __repr__(self):
        return f'{type(self).__name__}({self.ip_addr}:{self.ip_port})'

    def socket(self, flush=False, flush_timeout=None):
        """Return ``self`` as a context manager; ``flush`` drains pending input on entry."""
        self._flush_on_enter = flush
        self.temp_flush_timeout = flush_timeout
        return self

    def _connect(self):
        self._close()
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM, socket.IPPROTO_TCP)
        self.sock.settimeout(self.timeout)
        try:
            self.sock.connect((self.ip_addr, self.ip_port))
        except OSError:
            self.log.warning(f'{self!r}: error connecting to {self.ip_addr}:{self.ip_port}')
            raise IOError(f'{self!r}: timeout while connecting to {self.ip_addr}:{self.ip_port}')

    def _close(self):
        if self.sock:
            try:
                self.sock.shutdown(socket.SHUT_WR)
                self.sock.close()
            except OSError:
                pass
            self.sock = None

    def __enter__(self):
        if not self.sock:
            self._connect()
        self.socket_references += 1
        if self._flush_on_enter:
            old = self.sock.gettimeout()
            self.sock.settimeout(self.temp_flush_timeout or self.flush_timeout)
            try:
                while True:
                    try:
                        if not self.sock.recv(16384):
                            break
                    except socket.timeout:
                        break
            finally:
                self.sock.settimeout(old)
        return self.sock

    def __exit__(self, exc_type, exc, tb):
        if self.socket_references:
            self.socket_references -= 1
        if self.close_socket and not self.socket_references:
            self._close()

    def send(self, string):
        for trial in range(1, 5):
            try:
                self.sock.send(string.encode())
                return
            except OSError:
                if trial > 3:
                    raise IOError(f'{self!r}: error sending to {self.ip_addr}:{self.ip_port}')
                self.log.warning(f'{self!r}: send error on trial {trial}, reconnecting...')
                self._connect()

    def recv(self, buffer_size=16384, timeout=None):
        for trial in range(1, 5):
            old = self.sock.gettimeout()
            try:
                if timeout:
                    self.sock.settimeout(timeout)
                data = self.sock.recv(buffer_size).decode(errors='ignore')
                if not data:
                    raise ConnectionError('empty read')
                return data
            except socket.timeout:
                raise IOError(f'{self!r}: timeout while waiting for socket data')
            except OSError:
                if trial > 3:
                    raise IOError(f'{self!r}: error receiving from {self.ip_addr}:{self.ip_port}')
                self.log.warning(f'{self!r}: receive error on trial {trial}, reconnecting...')
                self._connect()
            finally:
                if timeout and self.sock:
                    self.sock.settimeout(old)

    def flush(self, timeout=0.1):
        with self.socket(flush=True, flush_timeout=timeout):
            pass


def run_client(args, server_class=None, client_class=None, object_name='',
               server_config_path=None, parser=None):
    """Command-line driver: ``prog [config[:section]] [server_name] [command [args...]]``.

    Creates the client (and a local server when none answers), optionally sends the
    ``start`` command with the selected server config, runs an optional client command,
    then keeps the loop alive until Ctrl-C when running a server or ``--run`` is given.
    Returns ``(client, server)``.
    """
    parser = parser or argparse.ArgumentParser(description=object_name)
    parser.add_argument('args', type=str, nargs='*', default='', help='[[config] [server_name]] [command {args}]')
    parser.add_argument('-p', '--port', default=None, type=int, help='port used by the server')
    parser.add_argument('-n', '--host', default=None, type=str, help='server hostname')
    run = parser.add_mutually_exclusive_group()
    run.add_argument('--run', action='store_true', help='run until Ctrl-C (implicit when no command is given)')
    run.add_argument('--no-run', action='store_true', help='return right away even without a command')
    parser.add_argument('--no-start', action='store_true', help='do not send the config to the server')
    opts = parser.parse_args(args)
    sargs = list(opts.args)

    def is_client_method(s):
        return ':' not in s and '.' not in s and hasattr(client_class, s)

    config = None
    if sargs and not is_client_method(sargs[0]):
        config = load_yaml_config(sargs.pop(0))

    server_config = config
    if server_config_path and server_config:
        server_name = sargs.pop(0) if sargs and not is_client_method(sargs[0]) else None
        for name in server_config_path.split('.'):
            server_config = server_config[name]
        if not server_config:
            raise RuntimeError(f'No server configuration block under {server_config_path}')
        if not server_name:
            if len(server_config) != 1:
                raise RuntimeError(f'More than one server configuration in {server_config_path}; '
                                   f'specify a server name')
            server_name = next(iter(server_config))
        if server_name not in server_config:
            raise RuntimeError(f'Server {server_name!r} is not defined under {server_config_path}')
        server_config = server_config[server_name]

    command = None
    if sargs:
        if not is_client_method(sargs[0]):
            raise RuntimeError(f'Unknown command {sargs[0]!r}. Client commands are:\n' +
                               '\n'.join(n for n in dir(client_class) if not n.startswith('_')))
        command = sargs.pop(0)

    port = opts.port or (server_config or {}).get('port') or server_class.DEFAULT_PORT
    hostname = opts.host or (server_config or {}).get('hostname') or 'localhost'

    async def main():
        client = client_class(hostname=hostname, port=port)
        server = getattr(client, 'server', None)
        logger = logging.getLogger(__name__)
        if server_config and not opts.no_start:
            await asyncio.sleep(3)  # let a freshly created local server come up
            logger.debug('Sending start() with the server configuration')
            await client.start(**server_config)
        if command:
            logger.debug(f'Sending command {command}({", ".join(sargs)}) to {client!r}')
            reply = await getattr(client, command)(*sargs)
            print()
            print(yaml.safe_dump(reply))
        if opts.run or (server and not command and not opts.no_run):
            print('REST server is running. Waiting for REST commands' if server else 'REST client is running.')
            print('Press CTRL-C to exit')
            while True:
                await asyncio.sleep(1)
        return client, server

    client = server = None
    try:
        client, server = asyncio.run(main())
    except KeyboardInterrupt:
        print("\nI'm done. Bye!")
    return client, server
