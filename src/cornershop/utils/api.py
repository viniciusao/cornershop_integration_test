""" The request/response cycle is known because of the docs
https://documenter.getpostman.com/view/1992239/TVmMgxxp#c76cbd6d-bbac-4853-8d7a-ff86b76f7127
and through local docker server requests tests. So
there are no try/except, control flows statements or
any sort of fine grained control of the requests.
"""

import abc
import base64
import builtins
import enum
import json
import logging
import pathlib
from typing import Any, cast, Dict, Tuple, Union

import requests


class APIEnum(enum.auto):
    TOKEN = '/oauth/token'
    MERCHANTS = 'api/merchants'
    MERCHANTS_BY_ID = 'api/merchants/{}'
    PRODUCTS = 'api/products'
    """ API paths. """

    NAME = 'name'
    ID = 'id'
    MERCHANTS_INFO = 'merchants'
    """ API merchants response fields"""


class APIOpsInterface(abc.ABC):

    @abc.abstractmethod
    def _token(self) -> None:

        pass

    @staticmethod
    @abc.abstractmethod
    def _decode_credentials(
            credentials: Dict[builtins.str, builtins.str]
    ) -> Dict[builtins.str, builtins.str]:

        pass

    @abc.abstractmethod
    def merchant_info(self, merchant_name: builtins.str) -> Dict[
        builtins.str,
        Union[builtins.str, builtins.bool]
    ]:

        pass

    @abc.abstractmethod
    def update_merchant_info(
            self,
            merchant_name: builtins.str,
            property_to_change: builtins.str,
            value_to_assign: Any
    ) -> None:

        pass

    @staticmethod
    @abc.abstractmethod
    def _url_joiner(
            base_url: builtins.str,
            api_path: builtins.str
    ) -> builtins.str:

        pass

    @abc.abstractmethod
    def delete_merchant_info(
            self,
            merchant_name: builtins.str
    ) -> None:

        pass

    @abc.abstractmethod
    def send_product_data(
            self,
            product: Dict[builtins.str, Any]
    ) -> builtins.int:

        pass


class APIOps(APIOpsInterface):

    _API = APIEnum()

    def __init__(self, credentials_file: builtins.str, url: builtins.str) -> None:

        self.headers: Dict[builtins.str, builtins.str] = {}
        self._BASE_URL = url
        self.credentials = str(pathlib.Path(__file__).parent.parent.joinpath(credentials_file).resolve())
        self._token()

    def _token(self) -> None:

        base_url = self._url_joiner(
            self._BASE_URL, self._API.TOKEN
        )
        with open(self.credentials, 'r') as f:
            credentials = json.load(f)

        params = self._decode_credentials(credentials)
        r = requests.post(base_url, params=params)
        self.headers = {'token': f'Bearer {r.json()["access_token"]}'}

    @staticmethod
    def _decode_credentials(
            credentials: Dict[builtins.str, builtins.str]
    ) -> Dict[builtins.str, builtins.str]:

        for k, v in credentials.items():
            credentials[k] = base64.b64decode(v).decode()

        return credentials

    def merchant_info(self, merchant_name: builtins.str) -> Dict[
        builtins.str,
        Union[builtins.str, builtins.bool]
    ]:

        url = self._url_joiner(
            self._BASE_URL, self._API.MERCHANTS
        )
        r = requests.get(url, headers=self.headers)
        richards = list(
            filter(
                lambda x: merchant_name in
                          x[self._API.NAME] and self._API.ID in x,
                r.json()[self._API.MERCHANTS_INFO]
            )
        )
        return richards.pop()

    def update_merchant_info(
            self,
            merchant_name: builtins.str,
            property_to_change: builtins.str,
            value_to_assign: Any
    ) -> None:

        mi = self.merchant_info(merchant_name)
        mi[property_to_change] = value_to_assign
        url = self._url_joiner(
            self._BASE_URL,
            self._API.MERCHANTS_BY_ID.format(mi[self._API.ID])
        )
        requests.put(url, headers=self.headers, json=mi)

    @staticmethod
    def _url_joiner(
            base_url: builtins.str,
            api_path: builtins.str
    ) -> builtins.str:

        return ''.join(
            (base_url, api_path)
        )

    def delete_merchant_info(
            self,
            merchant_name: builtins.str
    ) -> None:

        mi = self.merchant_info(merchant_name)
        url = self._url_joiner(
            self._BASE_URL,
            self._API.MERCHANTS_BY_ID.format(mi[self._API.ID])
        )
        requests.delete(url, headers=self.headers)

    def send_product_data(
            self,
            product: Dict[builtins.str, Any]
    ) -> builtins.int:

        url = self._url_joiner(
            self._BASE_URL,
            self._API.PRODUCTS
        )
        r = requests.post(url, headers=self.headers, json=product)
        return r.status_code


class API:

    _API = APIEnum()

    def __init__(self, *, api: APIOpsInterface, logger: logging.Logger) -> None:
        self._api = api
        self._logger = logger

    def merchant_id(self, merchant_name: builtins.str) -> builtins.str:
        return cast(builtins.str, self._api.merchant_info(merchant_name)[self._API.ID])

    def update_merchant_info(
            self,
            merchant_name: builtins.str,
            property_to_change: builtins.str,
            value_to_assign: Any
    ) -> None:

        self._api.update_merchant_info(
            merchant_name,
            property_to_change,
            value_to_assign
        )

    def delete_merchant_info(self, merchant_name: builtins.str) -> None:
        self._api.delete_merchant_info(merchant_name)

    def send_products(
            self,
            product: Tuple[builtins.int, Dict[builtins.str, Any]]
    ) -> None:

        item_number, item = product
        response = self._api.send_product_data(item)
        if response == 200:
            self._logger.info(f'Ingested product number: {item_number}')
        else:
            self._logger.error(f'Product has not been ingested: {item}')
