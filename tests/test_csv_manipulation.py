from html.parser import HTMLParser

import numpy as np
import pandas as pd

BRANCHES = ['MM', 'RHSM', 'MORPHEUS']
BRANCHES_TOFILTER = ['MM', 'RHSM']
STOCKS = [0, 1, 2, 3, 4]
MOCK_BY_BRANCH = pd.DataFrame(np.array(BRANCHES), columns=['BRANCH'])
MOCK_WITH_PRICES = pd.DataFrame(np.array(STOCKS), columns=['STOCK'])
DATA_SKU = {'SKU': [12, 12, 12, 12], 'PRICE': [10, 20, 30, 40], 'BRANCH': ['MM', 'RHSM', 'RHSM', 'MM']}
MOCK_SKU = pd.DataFrame(DATA_SKU)
CONCAT_COLUMNS = ['CATEGORY', 'SUB', 'SUBSUB']
MOCK_CONCAT = pd.DataFrame(np.array([['A','B','C']]), columns=CONCAT_COLUMNS)
MOCK_HTML = pd.DataFrame(np.array(['<p>SOPA INSTANTANEA AL PRECIO DE 8 PZAS 12 PZA</p>']), columns=['DESCRIPTION'])
PACKAGE_UNITS = ['GR', 'KG', 'GRS', 'ML']
MOCK_PACKAGES = pd.DataFrame(np.array(['<p>CHORIZO OAXACA CERDO 1 KG.</p>', 'aksoaksoak 40 ML', 'aoskaoskaosk 300 GR']), columns=['DESCRIPTION'])
MOCK_PACKAGES_NAN = pd.DataFrame(np.array(['RACK TV ALMA 1UN', 'aksoaksoak 40 ML', 'aoskaoskaosk 20 GR']), columns=['DESCRIPTION'])


def test_filter_by_branch():
    mask = MOCK_BY_BRANCH['BRANCH'].isin(BRANCHES_TOFILTER)
    mock = MOCK_BY_BRANCH[mask]
    assert set(mock['BRANCH'].tolist()) == set(BRANCHES_TOFILTER)

def test_stock_greater_than_zero():
    mask = MOCK_WITH_PRICES['STOCK'] > 0
    mock = MOCK_WITH_PRICES[mask]
    assert min(mock['STOCK'].tolist()) > 0

def test_drop_duplication():
    mock = MOCK_SKU[MOCK_SKU.groupby(['SKU', 'BRANCH'])['PRICE'].transform('max') == MOCK_SKU['PRICE']]
    branch_to_price = {
        k: v for k, v in zip(
            mock['BRANCH'].values.tolist(),
            mock['PRICE'].values.tolist()
        )
    }
    max_branch_price = {'MM': 40, 'RHSM': 30}
    assert branch_to_price == max_branch_price

def test_concat_columns_values():
    MOCK_CONCAT['CATEGORIES'] = MOCK_CONCAT[CONCAT_COLUMNS].apply(
        lambda row: '|'.join(row.values.astype(str)).lower(), axis=1
    )
    assert 'CATEGORIES' in MOCK_CONCAT.columns.values

def test_drop_columns_values():
    MOCK_CONCAT['CATEGORIES'] = MOCK_CONCAT[CONCAT_COLUMNS].apply(
        lambda row: '|'.join(row.values.astype(str)).lower(), axis=1
    )
    MOCK_CONCAT.drop(columns=CONCAT_COLUMNS, axis=1, inplace=True)
    mock_concat_columns = MOCK_CONCAT.columns.values
    assert list(filter(lambda x: x not in mock_concat_columns, CONCAT_COLUMNS))

def test_html_removal():
    MOCK_HTML['DESCRIPTION'] = MOCK_HTML['DESCRIPTION'].str.replace(r'<[^<>]*>', '', regex=True)
    parser = HTMLParser()
    parser.feed(MOCK_HTML['DESCRIPTION'].tolist().pop())
    assert not parser.get_starttag_text()

def test_extract_package_info():
    regex = MOCK_PACKAGES['DESCRIPTION'].str.extract(r'(?i)\b(\d+(?:\.\d+)?)\s*(gr|ml|kg|grs)\b', expand=False)
    MOCK_PACKAGES['PACKAGES'] = pd.Series(regex.values.tolist()).str.join(' ')
    assert list(filter(lambda x: x.split().pop() in PACKAGE_UNITS, MOCK_PACKAGES['PACKAGES'].values.tolist()))

def test_nan_to_none():
    regex = MOCK_PACKAGES_NAN['DESCRIPTION'].str.extract(r'(?i)\b(\d+(?:\.\d+)?)\s*(gr|ml|kg|grs)\b', expand=False)
    MOCK_PACKAGES_NAN['PACKAGES'] = pd.Series(regex.values.tolist()).str.join(' ')
    print(MOCK_PACKAGES_NAN)
    MOCK_PACKAGES_NAN.loc[MOCK_PACKAGES_NAN['PACKAGES'].isna(), 'PACKAGES'] = None
    assert not MOCK_PACKAGES_NAN['PACKAGES'].values.tolist()[0]