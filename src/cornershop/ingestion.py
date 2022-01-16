import builtins
import enum
from multiprocessing import Pool
from typing import Any, Dict, List

import pandas as pd

from .set_up import IntegrationSetup
from .utils import (
    API,
    APIOps,
    CSVOps,
    IngestItem,
    PandasOperations
)


class CVSUsefulColNames(enum.auto):
    """ As it is a small sample, we can enumerate
    useful cols by just looking through the CSVs' head """

    BARCODE = 'EAN'
    BRANCH = 'BRANCH'
    BRAND = 'BRAND_NAME'
    ITEM_DESCRIPTION = 'ITEM_DESCRIPTION'
    ITEM_IMG = 'ITEM_IMG'
    ITEM_NAME = 'ITEM_NAME'
    CATEGORY_STREAM = 'CATEGORY_STREAM'
    CATEGORY = 'CATEGORY'
    SUB_CATEGORY = 'SUB_CATEGORY'
    SUB_SUB_CATEGORY = 'SUB_SUB_CATEGORY'
    PACKAGE = 'PACKAGE'
    PRICE = 'PRICE'
    SKU = 'SKU'
    STOCK = 'STOCK'



class Facade:

    def __init__(
            self,
            *,
            credentials_file: builtins.str,
            merchant_to_ingest_id: builtins.str,
            merchant_to_update: builtins.str,
            merchant_to_delete: builtins.str,
            url: builtins.str,
            items_batch: builtins.int = 10,
            product_branches: List[builtins.str],
            package_units: List[builtins.str]
    ):

        self.merchant_update = merchant_to_update
        self.merchant_delete = merchant_to_delete
        self.processes = items_batch
        self.setup = IntegrationSetup()
        self.credentials = str(self.setup.PARENT_DIR.joinpath(credentials_file).resolve())
        self.api = API(api=APIOps(self.credentials, url), logger=self.setup.LOGGER)
        self.manipulate_csv = CSVOps(
            PandasOperations(),
            products_csv=self.setup.products_csv_path,
            price_stock_csv=self.setup.prices_stock_csv_path,
            merchant_id=self.api.merchant_id(merchant_to_ingest_id)
        )
        self.col = CVSUsefulColNames()
        self.branches = product_branches
        self.units = package_units

    def main(self):

        # ---- CSV Manipulation operations ----
        self.setup.LOGGER.info('Filtering CSVs by branch...')
        self._filter_csvs_by_branches(
            self.col.BRANCH,
            self.branches
        )
        self.setup.LOGGER.info('Filtering Prices Stock CSV by stock greater than zero...')
        self.manipulate_csv.filter_by_stock_greater_than_zero(self.col.STOCK)
        # it will merge products and stocks csvs
        self.setup.LOGGER.info('Merging CSVs on SKU column, futhermore, it will drop duplicates...')
        df_without_duplicates = self._merge_dataframes_and_drop_duplicates(
            merge_on_key=self.col.SKU,
            group_by_columns=[self.col.BRANCH, self.col.SKU],
            apply_transform_op_into_col=self.col.PRICE
        )
        self.setup.LOGGER.info('Concating categories columns and dropping em\' next...')
        df_without_duplicates[self.col.CATEGORY_STREAM] = self._concat_columns_values_and_drop_it(
            df_without_duplicates,
            sep='|',
            cols=[self.col.CATEGORY, self.col.SUB_CATEGORY, self.col.SUB_SUB_CATEGORY],
            lower_strings=True
        )
        self.setup.LOGGER.info('Extracting package info on item description column...')
        df_without_duplicates[self.col.PACKAGE] = self._extract_package_info_in_description(
            df_without_duplicates,
            col_to_extract=self.col.ITEM_DESCRIPTION,
            units=self.units
        )
        self.setup.LOGGER.info('Converting NaN value in package column to empty string...')
        self.manipulate_csv.nan_to_empty_str(df_without_duplicates, column=self.col.PACKAGE)
        self.setup.LOGGER.info('CSV Ops is over...')

        # ---- Ingestion Middle Ops  ----
        self.setup.LOGGER.info('Ingestion Middle Ops has begun...')
        self.setup.LOGGER.info('Separating branches...')
        branches = self._separate_by_branch(df_without_duplicates)
        mm_branch_df = branches['MM']
        rhsm_branch_df = branches['RHSM']
        self.setup.LOGGER.info('Getting top 100 most expensive items by branch...')
        top_100_mm = self._top_100_most_expensive_products(mm_branch_df, self.col.PRICE)
        top_100_rhsm = self._top_100_most_expensive_products(rhsm_branch_df, self.col.PRICE)
        self.setup.LOGGER.info('Validating collected items...')
        mm_items = self._validate_items(top_100_mm)
        rhsm_items = self._validate_items(top_100_rhsm)
        self.setup.LOGGER.info('Comparing products\'s branches dictionaries and merging...')
        items = self.manipulate_csv.compare_branchs(mm_items, rhsm_items, column=self.col.BRANCH)
        self.setup.LOGGER.info('Middle ops has finished...')
        # requests tasks
        self.setup.LOGGER.info('Doing requests tasks...')
        self.api.update_merchant_info(
            self.merchant_update,
            property_to_change='is_active',
            value_to_assign=True
        )
        self.api.delete_merchant_info(self.merchant_delete)
        self.setup.LOGGER.info(
            f'merchant\'s infos of {self.merchant_update} and '
            f'{self.merchant_delete} have been updated and deleted respectively'
        )

        # ---- INGEST ----
        self.setup.LOGGER.info('Ingestion has been initiated...')
        items_enumerated = [(c, i) for c, i in enumerate(items, start=1)]
        p = Pool(processes=self.processes)
        p.map(self.api.send_products, items_enumerated)
        self.setup.LOGGER.info(f'All top 100 most expensive products from branches <{self.branches}> have been ingested.')

    def _filter_csvs_by_branches(
            self,
            column: builtins.str,
            branches: List[builtins.str],
            *,
            dataframe: pd.DataFrame = None,
            return_dataframe: builtins.bool = False

    ) -> pd.DataFrame:

        return self.manipulate_csv.filter_by_branch(
            column,
            branches,
            dataframe=dataframe,
            return_dataframe=return_dataframe
        )


    def _merge_dataframes_and_drop_duplicates(
            self,
            merge_on_key: builtins.str,
            group_by_columns: List[builtins.str],
            apply_transform_op_into_col: builtins.str
    ) -> pd.DataFrame:

        df_merged = self.manipulate_csv.dataframes_merge_on(merge_on_key)
        return self.manipulate_csv.drop_duplications(
            df_merged,
            group_by_columns,
            apply_transform_op_into_col
        )

    def _concat_columns_values_and_drop_it(
            self,
            dataframe: pd.DataFrame,
            sep: builtins.str,
            cols: List[builtins.str],
            lower_strings: builtins.bool
    ) -> pd.DataFrame:

        mask = self.manipulate_csv.concat_columns_values(
            dataframe,
            sep,
            cols_to_concat=cols,
            lower=lower_strings
        )
        self.manipulate_csv.drop_columns_values(
            dataframe,
            cols
        )
        return mask

    def _extract_package_info_in_description(
            self,
            dataframe: pd.DataFrame,
            col_to_extract: builtins.str,
            units: List[builtins.str]
    ) -> pd.DataFrame:

        dataframe[col_to_extract] = self.manipulate_csv.remove_html_tags(
            dataframe,
            col_to_extract
        )
        regex_mask = self.manipulate_csv.extract_package_info(
            dataframe,
            col_to_extract,
            units
        )
        packages_mask = pd.Series(regex_mask.values.tolist(), index=regex_mask.index).str.join(' ')
        return packages_mask

    def _separate_by_branch(
            self,
            dataframe: pd.DataFrame
    ) -> Dict[builtins.str, Any]:

        branches = {}
        for branche in self.branches:
            branches[branche] = dataframe[self._filter_csvs_by_branches(
                    self.col.BRANCH,
                    [branche],
                    dataframe=dataframe,
                    return_dataframe=True
                )
            ]

        return branches

    @staticmethod
    def _top_100_most_expensive_products(
            dataframe: pd.DataFrame,
            sort_by_column: builtins.str,
    ) -> pd.DataFrame:

        return dataframe.sort_values(by=sort_by_column, ascending=False)[:100]

    def _validate_items(
            self,
            dataframe: pd.DataFrame
    ) -> Dict[builtins.str, Any]:

        items = {}
        for _, row in dataframe.iterrows():
            sku = str(row[self.col.SKU])
            model = IngestItem(
                merchant_id=self.manipulate_csv.merchant_id,
                sku=sku,
                barcodes=[str(row[self.col.BARCODE])],
                brand=row[self.col.BRAND],
                name=row[self.col.ITEM_NAME],
                description=row[self.col.ITEM_DESCRIPTION],
                package=row[self.col.PACKAGE],
                image_url=row[self.col.ITEM_IMG],
                category=row[self.col.CATEGORY_STREAM],
                url=row[self.col.ITEM_IMG],
                branch_products=[{
                    'branch': row[self.col.BRANCH],
                    'price': row[self.col.PRICE],
                    'stock': row[self.col.STOCK]
                }]
            )
            items[sku] = model.__dict__

        return items
