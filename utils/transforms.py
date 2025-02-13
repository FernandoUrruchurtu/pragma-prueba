from datetime import datetime

import pandas as pd
from typing import Tuple, Dict

def row_dict_transform(row:pd.Series) -> Tuple[Dict, Dict]:
    """Transforma una serie en una tupla de dos diccionarios

    Args:
        row (Pandas series): fila que es una serie de pandas

    Returns:
        Tuple[Dict, Dict]: Tupla con dos diccionarios, uno para ventas y otro generando un diccionario
    """

    # Calcula los rows de la tabla sales
    
    rows = {
        "user_id":row.user_id,
        "timestamp":row.timestamp.strftime('%Y-%m-%d'),
        "price":None if pd.isna(row.price) else row.price, ## Evalua si el precio tiene NA o no.
        "load_date":row.load_date.strftime('%Y-%m-%d %H:%M:%S')
    }

    row_t = row.timestamp

    calendar_rows = {
        "timestamp":row_t.strftime('%Y-%m-%d'),
        "week_day":row_t.weekday(),
        "day":row_t.day,
        "month":row_t.month,
        "year":row_t.year
    }

    return rows, calendar_rows


def add_timestamp(df:pd.DataFrame) -> pd.DataFrame:

    """Adiciona al dataframe una columna de tipo timestamp con la fecha de carga actual
    
    Returns:
        DataFrame: Data Frame con la columna 'load_date' con el dia de hoy.
    """

    df["load_date"] = pd.to_datetime("now")
    
    return df

def parse_date(df:pd.DataFrame) -> pd.DataFrame:

    """Transforma la columna timestamp a tipo datetime

    Returns:
        DataFrame: Data Frame con la columna 'timestamp' en formato datetime.
    """

    df["timestamp"] = pd.to_datetime(df["timestamp"], format="%m/%d/%Y")
    
    
    return df

