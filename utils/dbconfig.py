import os
from dotenv import load_dotenv


def _env(name:str) -> str:
    """Funcion privada para encontrar la variable de entorno designada

    Args:
        name (str): Descripcion de la variable

    Returns:
        str: retorna el valor de la variable
    """
    env_var = os.getenv(name)

    if env_var is None:
        load_dotenv(override=True)

        return os.getenv(name)
    
    return env_var

DATABASE_NAME = _env("DATABASE_NAME")
DB_HOST = _env("DB_HOST")
DB_USER = _env("DB_USER")
DB_PASSWORD = _env("DB_PASSWORD")
