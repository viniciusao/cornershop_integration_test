import builtins
import json
import logging
from multiprocessing import Pool
import os
import pathlib
import sys
from typing import BinaryIO, Optional, Sequence, Tuple

import requests


class IntegrationSetup:

    __CSVs_URL = {
        'products': 'https://cornershop-scrapers-'
                    'evaluation.s3.amazonaws.com/'
                    'public/PRODUCTS.csv',
        'prices_stock': 'https://cornershop-'
                        'scrapers-evaluation.s3.'
                        'amazonaws.com/public/'
                        'PRICES-STOCK.csv',
    }

    LOGGER = logging.getLogger('cornershop_integration')

    PARENT_DIR = pathlib.Path(__file__).parent

    @property
    def products_csv_path(self) -> builtins.str:
        return str(self.PARENT_DIR.joinpath(
                self.__csv_path_joiner(
                    self._csv_assets_dir,
                    self._products_csv_name
                )
            )
        )

    @property
    def prices_stock_csv_path(self) -> builtins.str:
        return str(self.PARENT_DIR.joinpath(
                self.__csv_path_joiner(
                    self._csv_assets_dir,
                   self._prices_stock_csv_name
                )
            )
        )

    def __init__(self) -> None:
        # ---- SETUP CSVs FILEs NAMEs AND PATHs ----
        self._products_csv_name = self.__CSVs_URL['products'].split('/').pop()
        self._prices_stock_csv_name = self.__CSVs_URL['prices_stock'].split('/').pop()
        self._csv_assets_dir = 'assets'
        self._csvs_path = [
            self.products_csv_path,
            self.prices_stock_csv_path,
        ]

        # --- Logger ---
        self.LOGGER.setLevel(logging.INFO)
        sh = logging.StreamHandler(stream=sys.stdout)
        fmt = logging.Formatter(
            fmt='%(asctime)s %(levelname)-4s '
                '[%(filename)s:%(lineno)s] %(message)s',
            datefmt='%Y-%m-%d:%H:%M:%S'
        )
        sh.setFormatter(fmt)
        self.LOGGER.addHandler(sh)

    @staticmethod
    def __csv_path_joiner(
            asset_path: builtins.str,
            csv_name: builtins.str
    ) -> builtins.str:

        return '/'.join((asset_path, csv_name))

    def main(self) -> None:
        not_cfe = self.__check_csvs_existence()
        if not_cfe:
            self.__download_csv_files()

    def __check_csvs_existence(self) -> Sequence[Optional[builtins.str]]:

        notfound = []
        for csv_path in self._csvs_path:
            if not os.path.exists(csv_path):
                notfound.append(csv_path)

        return notfound

    def __download_csv_files(self) -> None:
        self.__check_dir_existence(self._csv_assets_dir)
        urls = self.__CSVs_URL.values()
        zip_ = [
            (urls_, csvs_path) for
            urls_, csvs_path in
            zip(urls, self._csvs_path)
        ]
        p = Pool(processes=5)
        p.map(self.download, zip_)

    def download(
            self,
            url_and_csv_path: Tuple[builtins.str, builtins.str]
    ) -> None:

        url, csv_filepath = url_and_csv_path
        with open(csv_filepath, 'w+b') as f:
            self.LOGGER.info(
                f'{csv_filepath} has not been found.'
                ' Downloading has been started.'
            )
            try:
                r = requests.get(
                    url,
                    stream=True
                )
            except Exception as e:
                self.LOGGER.exception(e.args[0])

            else:
                if not self.__process_downloading_stream(
                        file_obj=f,
                        file_path=csv_filepath,
                        stream=r
                ):

                    self.LOGGER.error(
                        f'{csv_filepath}'
                        'is not complete.'
                    )
                else:
                    sys.stdout.write(
                    f'\n{csv_filepath} '
                    'Download completed.\n'
                    )

    def __check_dir_existence(
            self,
            directory: builtins.str
    ) -> None:

        assets_dir = self.PARENT_DIR.joinpath(
            self._csv_assets_dir)

        if not os.path.exists(assets_dir):
            os.makedirs(assets_dir)

    def __process_downloading_stream(
            self,
            file_obj: BinaryIO,
            file_path: builtins.str,
            stream: requests.Response
    ) -> builtins.bool:

        file_size = int(
            stream.headers.get('content-length')
        )
        self.LOGGER.info(
            json.dumps(
                {
                    'csv name': file_path,
                    'csv size (MB)': file_size / (1024 * 1000)
                }
            )
        )

        chunks_downloaded_sofar = 0
        csv_filobj_cursor_position = file_obj.tell()
        for chunk in stream.iter_content(chunk_size=4096):
            chunks_downloaded_sofar += len(chunk)
            file_obj.write(chunk)
            progress_bar = int(
                25 * chunks_downloaded_sofar / file_size
            )
            sys.stdout.write(
                '\r[%s%s]' % (
                    '=' * progress_bar,
                    ' ' * (25 - progress_bar)
                )
            )
            sys.stdout.flush()

        else:
            total_chunks_downloaded = chunks_downloaded_sofar

        file_obj.seek(csv_filobj_cursor_position)
        csv_content_length = len(file_obj.read())
        return total_chunks_downloaded == csv_content_length
