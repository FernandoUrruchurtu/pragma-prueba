import pandas as pd
from typing import Tuple, Dict

def row_loads(row:pd.Series) -> Tuple[Dict, Dict]:
    """_summary_

    Args:
        row (pd.Series): _description_

    Returns:
        Tuple[Dict, Dict]: _description_
    """

    rows = {
        "user_id":row.user_id,
        "timestamp":row.timestamp.strftime('%Y-%m-%d'),
        "price":row.price,
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

    """_summary_

    Returns:
        _type_: _description_
    """

    df["load_date"] = pd.to_datetime("now")
    return df

def parse_date(df:pd.DataFrame) -> pd.DataFrame:

    """_summary_

    Returns:
        _type_: _description_
    """

    df["timestamp"] = pd.to_datetime(df["timestamp"], format="%m/%d/%Y")
    return df

