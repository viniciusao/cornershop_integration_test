import pytest
import requests
from tests import api_paths, CREDENTIALS

BASE_URL = CREDENTIALS.pop('base_url')
HEADERS = {}
RICHARD_INFO = {}

@pytest.mark.dependency()
def test_get_credentials():
    global HEADERS
    base_url = ''.join(
        (BASE_URL, api_paths.TOKEN,)
    )
    params = CREDENTIALS
    r = requests.post(base_url, params=params)
    assert 'access_token' in r.json()
    HEADERS = {'token': f'Bearer {r.json()["access_token"]}'}

@pytest.mark.dependency(depends=['test_get_credentials'])
def test_get_richards_info():
    global RICHARD_INFO
    base_url = ''.join(
        (BASE_URL, api_paths.MERCHANTS)
    )
    r = requests.get(base_url, headers=HEADERS)
    richard_info = list(filter(lambda x: 'Richard\'s' in x['name'] and 'id' in x, r.json()['merchants']))
    assert richard_info
    RICHARD_INFO = richard_info.pop()

@pytest.mark.dependency(depends=['test_get_richards_info'])
def test_update_richards_id_toactivate():
    richards_info = RICHARD_INFO
    old_ = richards_info['is_active']
    richards_info['is_active'] = True
    base_url = ''.join(
        (BASE_URL, api_paths.MERCHANTS_BY_ID.format(richards_info['id']))
    )
    r = requests.put(base_url, json=richards_info, headers=HEADERS)
    assert old_ != r.json()['is_active']

@pytest.mark.dependency(depends=['test_get_credentials'])
def test_delete_beauty_id():
    base_url = ''.join(
        (BASE_URL, api_paths.MERCHANTS)
    )
    r = requests.get(base_url, headers=HEADERS)
    beauty_info = list(
        filter(
            lambda x: 'Beauty' in x['name'] and 'id' in x,
            r.json()['merchants']
        )
    ).pop()
    base_url = ''.join(
        (BASE_URL, api_paths.MERCHANTS_BY_ID.format(beauty_info['id']))
    )
    r = requests.delete(base_url, headers=HEADERS)
    assert r.status_code == 200 and not len(r.text)
