import abc
import builtins
from typing import Any, Dict, List, Optional

import pandas as pd


class PandasOpsInterface(abc.ABC):

    @staticmethod
    @abc.abstractmethod
    def filter_by_branches(
            dataframe: pd.DataFrame,
            column: builtins.str,
            branches: List[builtins.str]
    ) -> pd.DataFrame:

        pass

    @staticmethod
    @abc.abstractmethod
    def filter_by_stock_greater_than_zero(
            dataframe: pd.DataFrame,
            column: builtins.str
    ) -> pd.DataFrame:

        pass

    @staticmethod
    @abc.abstractmethod
    def dataframes_merge_on(
            *dataframes: pd.DataFrame,
            on_key: builtins.str
    ) -> pd.DataFrame:

        pass

    @staticmethod
    @abc.abstractmethod
    def drop_duplications(
            dataframe: pd.DataFrame,
            group_by_columns: List[builtins.str],
            apply_transform_into_col: builtins.str,
            transform_op: builtins.str = 'max'
    ) -> pd.DataFrame:

        pass

    @staticmethod
    @abc.abstractmethod
    def concat_columns_values(
            dataframe: pd.DataFrame,
            sep: builtins.str,
            cols_to_concat: List[builtins.str],
            lower: builtins.bool = None
    ) -> pd.DataFrame:

        pass

    @staticmethod
    @abc.abstractmethod
    def drop_columns_values(
            dataframe: pd.DataFrame,
            cols_to_drop: List[builtins.str]
    ) -> None:

        pass

    @staticmethod
    @abc.abstractmethod
    def remove_html_tags(
            dataframe: pd.DataFrame,
            column: builtins.str,
    ) -> pd.DataFrame:

        pass

    @staticmethod
    @abc.abstractmethod
    def extract_package_info(
            dataframe: pd.DataFrame,
            column: builtins.str,
            units: List[builtins.str]
    ) -> pd.DataFrame:

        pass

    @staticmethod
    @abc.abstractmethod
    def nan_to_empty_str(
            dataframe: pd.DataFrame,
            column: builtins.str
    ) -> None:

        pass


class PandasOperations(PandasOpsInterface):

    @staticmethod
    def filter_by_branches(
            dataframe: pd.DataFrame,
            column: builtins.str,
            branches: List[builtins.str]
    ) -> pd.DataFrame:

        return dataframe[column].isin(branches)

    @staticmethod
    def filter_by_stock_greater_than_zero(
            dataframe: pd.DataFrame,
            column: builtins.str
    ) -> pd.DataFrame:

        return dataframe[column] > 0

    @staticmethod
    def dataframes_merge_on(
            *dataframes: pd.DataFrame,
            on_key: builtins.str
    ) -> pd.DataFrame:

        return pd.merge(*dataframes, on=on_key)

    @staticmethod
    def drop_duplications(
            dataframe: pd.DataFrame,
            group_by_columns: List[builtins.str],
            apply_transform_into_col: builtins.str,
            transform_op: builtins.str = 'max'
    ) -> pd.DataFrame:

        return dataframe[
            dataframe.groupby(group_by_columns)[apply_transform_into_col].
                transform(transform_op) == dataframe[apply_transform_into_col]
        ]

    @staticmethod
    def concat_columns_values(
            dataframe: pd.DataFrame,
            sep: builtins.str,
            cols_to_concat: List[builtins.str],
            lower: builtins.bool = None
    ) -> pd.DataFrame:

        if lower:
            return dataframe[cols_to_concat].apply(
                lambda row: sep.join(
                    row.values.astype(str)).lower(), axis=1)

        return dataframe[cols_to_concat].apply(
            lambda row: sep.join(row.values.astype(str)), axis=1)


    @staticmethod
    def drop_columns_values(
            dataframe: pd.DataFrame,
            cols_to_drop: List[builtins.str]
    ) -> None:

        dataframe.drop(columns=cols_to_drop, axis=1, inplace=True)

    @staticmethod
    def remove_html_tags(
            dataframe: pd.DataFrame,
            column: builtins.str,
    ) -> pd.DataFrame:

        return dataframe[column].str.replace(r'<[^<>]*>', '', regex=True)

    @staticmethod
    def extract_package_info(
            dataframe: pd.DataFrame,
            column: builtins.str,
            units: List[builtins.str]
    ) -> pd.DataFrame:

        return dataframe[column].str.extract(rf'(?i)\b(\d+(?:\.\d+)?)\s*({"|".join(units).lower()})\b', expand=False)

    @staticmethod
    def nan_to_empty_str(
            dataframe: pd.DataFrame,
            column: builtins.str
    ) -> None:

        dataframe.loc[dataframe[column].isna(), column] = ''


class CSVOps:

    def __init__(
            self,
            pandas_ops_interface: PandasOpsInterface,
            *,
            products_csv: builtins.str,
            price_stock_csv: builtins.str,
            merchant_id: builtins.str
    ) -> None:

        self._pandas_ops = pandas_ops_interface
        # suppress warnings.
        pd.set_option('mode.chained_assignment', None)
        self.products = pd.read_csv(products_csv, sep='|')
        self.stock = pd.read_csv(price_stock_csv, sep='|')
        self.merchant_id = merchant_id

    def filter_by_branch(
            self,
            column: builtins.str,
            values: List[builtins.str],
            *,
            dataframe: pd.DataFrame = None,
            return_dataframe: builtins.bool = False
    ) -> Optional[pd.DataFrame]:

        mask = self._pandas_ops.filter_by_branches(
            self.stock if not isinstance(dataframe, pd.DataFrame) else dataframe,
            column,
            values
        )

        if return_dataframe:
            return mask

        self.stock = self.stock[mask]

    def filter_by_stock_greater_than_zero(
            self,
            column: builtins.str
    ) -> None:

        mask = self._pandas_ops.filter_by_stock_greater_than_zero(
            self.stock,
            column
        )
        self.stock = self.stock[mask]

    def dataframes_merge_on(self, key: builtins.str) -> pd.DataFrame:
        return self._pandas_ops.dataframes_merge_on(
            self.products,
            self.stock,
            on_key=key
        )

    def drop_duplications(
            self,
            dataframe: pd.DataFrame,
            group_by_columns: List[builtins.str],
            apply_transform_into_col: builtins.str,
            transform_op: builtins.str = 'max'
    ) -> pd.DataFrame:

        return self._pandas_ops.drop_duplications(
            dataframe,
            group_by_columns,
            apply_transform_into_col,
            transform_op
        )

    def concat_columns_values(
            self,
            dataframe: pd.DataFrame,
            sep: builtins.str,
            cols_to_concat: List[builtins.str],
            lower: builtins.bool = None
    ) -> pd.DataFrame:

        return self._pandas_ops.concat_columns_values(
            dataframe,
            sep,
            cols_to_concat,
            lower
        )

    def drop_columns_values(
            self,
            dataframe: pd.DataFrame,
            cols_to_drop: List[builtins.str]
    ) -> None:

        self._pandas_ops.drop_columns_values(dataframe, cols_to_drop)

    def remove_html_tags(
            self,
            dataframe: pd.DataFrame,
            column: builtins.str,
    ) -> pd.DataFrame:

        return self._pandas_ops.remove_html_tags(dataframe, column)

    def extract_package_info(
            self,
            dataframe: pd.DataFrame,
            column: builtins.str,
            units: List[builtins.str]
    ) -> pd.DataFrame:

        return self._pandas_ops.extract_package_info(dataframe, column, units)

    def nan_to_empty_str(
            self,
            dataframe: pd.DataFrame,
            column: builtins.str
    ) -> None:

        self._pandas_ops.nan_to_empty_str(dataframe, column)

    @staticmethod
    def compare_branchs(
            branch_a: Dict[str, Any],
            branch_b: Dict[str, Any],
            *,
            column: builtins.str
    ) -> List[Dict[str, Any]]:
        """ I devised this algorithm to compare the
        top 100 most expensive products by branch (limited to 2) because
        a product SKU could be in both branches, then,
        both products would be merge into one. Nevertheless,
        this functions returns such top products in one list. """

        items = []
        branch_b_copy = branch_b.copy()
        for branch in branch_a:
            count_equals = 0
            for branch_ in branch_b:
                if branch_ == branch:
                    branch_a[branch][column] = branch_a[branch][column] + branch_b[branch_][column]
                    items.append(branch_a[branch])
                    branch_b_copy.pop(branch_)
                    count_equals += 1
            else:
                if not count_equals:
                    items.append(branch_a[branch])
        else:
            for branch in branch_b_copy.values():
                items.append(branch)

        return items
