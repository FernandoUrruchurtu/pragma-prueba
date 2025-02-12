import os
from typing import Generator

import pandas as pd

def list_datafile(path:str, validation=False) -> Generator[str, None, None]:
    """_summary_

    Args:
        path (str): _description_
        validation (bool, optional): _description_. Defaults to False.

    Yields:
        Generator[str, None, None]: _description_
    """

    files = sorted([
            os.path.join(path, i) for i in os.listdir(path) if i.endswith(".csv")
            ])

    if validation:

        file_list = [f for f in files if 'validation' in f]

    else:
        file_list = [f for f in files if 'validation' not in f]

    for file in file_list:

        yield file

def batch_processing(file:str, chunks:int) -> Generator[pd.DataFrame, None, None]:
    """_summary_

    Args:
        file (str): _description_
        chunks (int): _description_

    Yields:
        Generator[pd.DataFrame, None, None]: _description_
    """

    for chunk in pd.read_csv(file, chunksize=chunks):
        yield chunk