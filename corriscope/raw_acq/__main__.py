"""CLI entry point for the RawAcq REST server."""

import sys

from corriscope.common import log
from corriscope.common.rest import run_client

from .client import RawAcqAsyncRESTClient
from .server import RawAcqAsyncRESTServer


def main():
    log.setup_basic_logging('DEBUG')
    client, server = run_client(
        sys.argv[1:],
        RawAcqAsyncRESTServer,
        RawAcqAsyncRESTClient,
        object_name='RawAcq',
        server_config_path='raw_acq.servers')
    return client, server


if __name__ == '__main__':
    client, server = main()
