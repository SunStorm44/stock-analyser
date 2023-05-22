from itertools import zip_longest
import pandas as pd


def grouper(iterable, chunk_size, fillvalue=None):
    """Function used to split a list into user-defined chunks"""
    args = [iter(iterable)] * chunk_size
    return zip_longest(*args, fillvalue=fillvalue)


def process_symbols(raw_symbols: pd.DataFrame) -> dict:
    """Change symbols object format to align with the XTB output"""
    return raw_symbols.groupby('country')['ticker'].apply(list).to_dict()
