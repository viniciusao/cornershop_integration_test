import abc
import builtins
from typing import List

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
