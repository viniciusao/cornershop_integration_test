""" Command line tool for the three main components:
Integration Setup, API and CSV Manipulation ."""


import argparse
import base64
import builtins
import json
import os.path
import pathlib

from ..ingestion import Facade
from ..set_up import IntegrationSetup


def integration_setup() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--setup',
        dest='setup',
        action='store_true',
        help='Start setup of integration test.'
    )
    args = parser.parse_args()
    if args.setup:
        IntegrationSetup().main()

def oauth_setup() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--client-id',
        dest='client_id',
        action='store',
        required=True,
        type=builtins.str,
    )
    parser.add_argument(
        '--client-secret',
        dest='client_secret',
        action='store',
        required=True,
        type=builtins.str
    )
    parser.add_argument(
        '--oauth-grant-type',
        dest='grant_type',
        action='store',
        default='client_credentials',
        type=builtins.str
    )
    parser.add_argument(
        '--export',
        dest='filename',
        action='store',
        default='.config.json',
        help='File name where credentials will be stored. It must be a json.',
        type=builtins.str
    )
    args = parser.parse_args()
    credentials = args.filename
    if not credentials.endswith('.json'):
        raise ValueError('Filename must be a json!')
    if not credentials.startswith('.'):
        credentials = '.' + credentials

    credentials_path = pathlib.Path(__file__).parent.parent.joinpath(args.filename)

    with open(credentials_path, 'w') as f:
        f.write(json.dumps(
                {
                    'client_id': base64.b64encode(args.client_id.encode()).decode(),
                    'client_secret': base64.b64encode(args.client_secret.encode()).decode(),
                    'grant_type': base64.b64encode(args.grant_type.encode()).decode()
                }
            )
        )

def ingestion() -> None:
    parser = argparse.ArgumentParser(
        epilog='''Example\n ingestion --start (... required flags ...) [--branches/units BRANCH_1, BRANCH_2 ...]''',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        '--start',
        dest='ingest',
        action='store_true',
        required=True,
        help='Start ingestion'
    )
    parser.add_argument(
        '--merchant-ingest',
        dest='merchant_ingest',
        action='store',
        required=True,
        help='Merchant\'s id must be ingested with '
             'the products, hence, its name is necessary.',
        type=builtins.str
    )
    parser.add_argument(
        '--merchant-update',
        dest='merchant_update',
        action='store',
        required=True,
        help='Merchant\'s status will be updated '
             ', hence, its name is necessary.',
        type=builtins.str
    )
    parser.add_argument(
        '--merchant-delete',
        dest='merchant_delete',
        action='store',
        required=True,
        help='Merchant\'s info will be deleted '
             ', hence, its name is necessary.',
        type=builtins.str
    )
    parser.add_argument(
        '--credentials-file',
        dest='credentials_file',
        default='.config.json',
        type=builtins.str
    )
    parser.add_argument(
        '--url',
        dest='url',
        action='store',
        choices=['http://0.0.0.0:5000/', 'https://damp-mountain-80499.herokuapp.com/'],
        default='https://damp-mountain-80499.herokuapp.com/',
        help='Host to ingest products.',
        type=builtins.str
    )
    parser.add_argument(
        '--products-batch',
        dest='items_batch',
        action='store',
        default=10,
        help='How many to ingest simultaneously.',
        type=builtins.int
    )
    parser.add_argument(
        '--branches',
        dest='branches',
        action='store',
        nargs='+',
        default=['MM', 'RHSM'],
        help='Product branches.',
        type=builtins.list
    )
    parser.add_argument(
        '--package-units',
        dest='units',
        action='store',
        nargs='+',
        default=['GR', 'ML', 'KG', 'GRS'],
        help='Package units to extract.',
        type=builtins.list
    )
    args = parser.parse_args()
    credential_file = args.credentials_file
    if not os.path.exists(pathlib.Path(__file__).parent.parent.joinpath(credential_file).resolve()):
        raise ValueError('credentials file doesn\'t exist.')
    if args.ingest:
        Facade(
            credentials_file=credential_file,
            merchant_to_ingest_id=args.merchant_ingest,
            merchant_to_update=args.merchant_update,
            merchant_to_delete=args.merchant_delete,
            url=args.url,
            items_batch=args.items_batch,
            product_branches=args.branches,
            package_units=args.units
        ).main()