import json
import os
import sys

import requests

from . import csv_downloaded_dir, i, parent_dir


def test_download_csv_files():
    small_csv_file_example = 'https://perso.telecom-' \
                             'paristech.fr/eagan/' \
                             'class/igr204/data/cars.csv'

    filename = i.products_csv_path.split('/').pop()
    pd = parent_dir.joinpath(csv_downloaded_dir)
    if not os.path.exists(pd):
        os.makedirs(pd)

    with open(f'{pd.joinpath(filename)}', 'w+b') as f:
        i.LOGGER.info(
            f'{filename} has not been found. '
            f'Downloading has been started...'
        )

        try:
            r = requests.get(
                small_csv_file_example,
                stream=True
            )
        except Exception as e:
            print(e.args[0])
        else:
            file_size = int(
                r.headers.get('content-length'))
            i.LOGGER.info(
                json.dumps(
                    {
                        'file ext': r.headers['content-type'],
                        'file size (MB)': file_size / (1024 * 1000)
                    }
                )
            )
            how_much_data_downloaded_sofar = 0
            initial_position = f.tell()
            for data in r.iter_content(chunk_size=1024):
                how_much_data_downloaded_sofar += len(data)
                f.write(data)
                progress_bar = int(
                    50 * how_much_data_downloaded_sofar / file_size
                )
                sys.stdout.write(
                    '\r[%s%s]' % (
                        '=' * progress_bar,
                        ' ' * (50 - progress_bar)
                    )
                )
                sys.stdout.flush()
            else:
                total_downloaded_data = how_much_data_downloaded_sofar
            f.seek(initial_position)
            length_downloaded_data = len(f.read())

    assert total_downloaded_data == length_downloaded_data, 'File not complete.'

def test_check_files_existence():
    asset_path = parent_dir.joinpath(csv_downloaded_dir).joinpath(i.products_csv_path.split('/').pop())
    files_path = [
        # we shall go only with one
        asset_path,
    ]
    notfound = []
    for fp in files_path:
        if not os.path.exists(fp):
            notfound.append(fp)

    assert not notfound, 'Files not found in ' \
                     f'current dir: {notfound}'

    os.remove(asset_path)