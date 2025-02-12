import os

from utils.functions import list_datafile, batch_processing


from utils.transforms import add_timestamp, parse_date, row_loads

from database.database import Database

from dotenv import load_dotenv


load_dotenv(dotenv_path='./db.env')

USER = os.getenv('DB_USER')
PWD = os.getenv('DB_PASSWORD')
HOST = os.getenv('DB_HOST')
DB = os.getenv('DATABASE_NAME')

CHUNKSIZE = 3

plsql_db = Database(HOST, USER, PWD, DB)

pathlist = list_datafile(path='./data')

for file in pathlist:

    df_gen = batch_processing(file, chunks=CHUNKSIZE)

    for ch in df_gen:

        df =(
            ch.pipe(add_timestamp)
            .pipe(parse_date)
            )
            
        for row in df.itertuples(index=False):

            row, calendar_row = row_loads(row)

            plsql_db.insert(calendar_row, 'calendar')

            plsql_db.insert(row, 'sales')

            print(f'-> inserting values in file {file}')