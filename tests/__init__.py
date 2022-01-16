import pathlib

from src.cornershop.set_up import IntegrationSetup
from src.cornershop.utils.api import APIEnum

csv_downloaded_dir = 'assets'
parent_dir = pathlib.Path(__file__).parent
i = IntegrationSetup()

CREDENTIALS = {
    'base_url': 'https://damp-mountain-80499.herokuapp.com/',
    'client_id': 'mRkZGFjM',
    'client_secret': 'ZGVmMjMz',
    'grant_type': 'client_credentials'
}
api_paths = APIEnum()
