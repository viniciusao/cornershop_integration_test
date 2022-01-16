import abc
import builtins
import enum
from typing import Any, Dict, Union


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

        raise NotImplemented

    @staticmethod
    @abc.abstractmethod
    def _decode_credentials(
            credentials: Dict[builtins.str, builtins.str]
    ) -> Dict[builtins.str, builtins.str]:

        raise NotImplemented

    @abc.abstractmethod
    def merchant_info(self, merchant_name: builtins.str) -> Dict[
        builtins.str,
        Union[builtins.str, builtins.bool]
    ]:

        raise NotImplemented

    @abc.abstractmethod
    def update_merchant_info(
            self,
            merchant_name: builtins.str,
            property_to_change: builtins.str,
            value_to_assign: Any
    ) -> None:

        raise NotImplemented

    @staticmethod
    @abc.abstractmethod
    def _url_joiner(
            base_url: builtins.str,
            api_path: builtins.str
    ) -> builtins.str:

        raise NotImplemented

    @abc.abstractmethod
    def delete_merchant_info(
            self,
            merchant_name: builtins.str
    ) -> None:

        raise NotImplemented

    @abc.abstractmethod
    def send_product_data(
            self,
            product: Dict[builtins.str, Any]
    ) -> builtins.int:

        raise NotImplemented
