import os
from dotenv import load_dotenv


def _env(name:str) -> str:

    env_var = os.getenv(name)

    if env_var is None:
        load_dotenv(override=True)

        return os.getenv(name)
    
    return env_var

DATABASE_NAME = _env("DATABASE_NAME")
DB_HOST = _env("DB_HOST")
DB_USER = _env("DB_USER")
DB_PASSWORD = _env("DB_PASSWORD")
