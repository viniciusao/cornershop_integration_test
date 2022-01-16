import pandas as pd
from typing import List
import builtins
import abc

class PandasOpsInterface(abc.ABC):

    @staticmethod
    @abc.abstractmethod
    def filter_by_branches(
            dataframe: pd.DataFrame,
            column: builtins.str,
            branches: List[builtins.str]
    ) -> pd.DataFrame:

        raise NotImplemented

    @staticmethod
    @abc.abstractmethod
    def filter_by_stock_greater_than_zero(
            dataframe: pd.DataFrame,
            column: builtins.str
    ) -> pd.DataFrame:

        raise NotImplemented

    @staticmethod
    @abc.abstractmethod
    def dataframes_merge_on(
            *dataframes: pd.DataFrame,
            on_key: builtins.str
    ) -> pd.DataFrame:

        raise NotImplemented

    @staticmethod
    @abc.abstractmethod
    def drop_duplications(
            dataframe: pd.DataFrame,
            group_by_columns: List[builtins.str],
            apply_transform_into_col: builtins.str,
            transform_op: builtins.str = 'max'
    ) -> pd.DataFrame:

        raise NotImplemented

    @staticmethod
    @abc.abstractmethod
    def concat_columns_values(
            dataframe: pd.DataFrame,
            sep: builtins.str,
            cols_to_concat: List[builtins.str],
            lower: builtins.bool = None
    ) -> pd.DataFrame:

        raise NotImplemented

    @staticmethod
    @abc.abstractmethod
    def drop_columns_values(
            dataframe: pd.DataFrame,
            cols_to_drop: List[builtins.str]
    ) -> None:

        raise NotImplemented

    @staticmethod
    @abc.abstractmethod
    def remove_html_tags(
            dataframe: pd.DataFrame,
            column: builtins.str,
    ) -> pd.DataFrame:

        raise NotImplemented

    @staticmethod
    @abc.abstractmethod
    def extract_package_info(
            dataframe: pd.DataFrame,
            column: builtins.str,
            units: List[builtins.str]
    ) -> pd.DataFrame:

        raise NotImplemented

    @staticmethod
    @abc.abstractmethod
    def nan_to_empty_str(
            dataframe: pd.DataFrame,
            column: builtins.str
    ) -> None:

        raise NotImplemented