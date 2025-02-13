import os
from typing import Generator

import pandas as pd

def list_datafile(path:str, validation=False) -> Generator[str, None, None]:
    """Lista los archivos dentro de un directorio, sin almacenar la lista en memoria

    Args:
        path (str): directorio para listar los archivos
        validation (bool, optional): flag para decidir si incluir o no los archivos que contengan la cadena 'validation' . Defaults to False.

    Yields:
        Generator: Generador con el objeto como secuencia de archivos
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
    """Lee los archivos en microbatches de tamaño chunks

    Args:
        file (str): nombre del archivo a leer
        chunks (int): tamaño de los microbatches

    Yields:
        Generator[pd.DataFrame, None, None]: Generador del objeto con los microbatches
    """

    for chunk in pd.read_csv(file, chunksize=chunks):
        yield chunk