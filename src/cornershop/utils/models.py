import builtins
from dataclasses import dataclass
from typing import Dict, List, Union


@dataclass
class IngestItem:

    merchant_id: builtins.str
    sku: builtins.str
    barcodes: List[builtins.str]
    brand: builtins.str
    name: builtins.str
    description: builtins.str
    package: builtins.str
    image_url: builtins.str
    category: builtins.str
    url: builtins.str
    branch_products: List[Dict[builtins.str, Union[builtins.str, builtins.int, builtins.float]]]

    def __post_init__(self) -> None:
        if not isinstance(self.barcodes, builtins.list) or not len(self.barcodes) > 0 \
                or not list(filter(lambda x: isinstance(x, builtins.str), self.barcodes)):

            raise ValueError('`barcodes property must be a list of length > 0. consisted of strings.')

        exception = ValueError(
            '`branch_products` must be a list of length > 0 '
            'consisted of dict of three string keys and values: string, int and float'
        )
        try:
            if not isinstance(self.branch_products, builtins.list) \
                    or not len(self.branch_products) > 0 \
                    or not isinstance(self.branch_products[0], builtins.dict) \
                    or not list(filter(lambda x: isinstance(x, builtins.str), self.branch_products[0].keys())) \
                    or not list(filter(lambda x: isinstance(x, builtins.str) or isinstance(x, builtins.int) or isinstance(x, builtins.float), self.branch_products[0].values())):

                raise exception

        except IndexError as e:
            raise exception

        if not all([
            isinstance(self.merchant_id, builtins.str),
            isinstance(self.sku, builtins.str),
            isinstance(self.name, builtins.str),
            isinstance(self.description, builtins.str),
            isinstance(self.package, builtins.str),
            isinstance(self.image_url, builtins.str),
            isinstance(self.category, builtins.str),
            isinstance(self.url, builtins.str)
        ]):

            raise ValueError(
                '`merchant_id, sku, brand, name, description, '
                'package, image_url, category, url` properties '
                'must be strings.'
            )
