import time
## Clases para tipado
from typing  import Generator
from logging import Logger
## Configuracion
import utils.dbconfig as dbc
## Funciones
from utils.functions   import list_datafile, batch_processing
from utils.transforms  import add_timestamp, parse_date, row_dict_transform
## Clases necesarias
from utils.logs        import PipelineLogs
from database.database import Database

def pipeline(
        file_list:Generator[str, None, None], 
        plsqldb:Database, 
        chunks:int, 
        logs:Logger
        ):

    """
    Pipeline para insercion y mantenimiento de los datos
    """
    
    tl_file = []
    for file in file_list:

        t_file = time.time()
        logs.info(f"Leyendo archivo {file}")

        df_gen = batch_processing(file, chunks=chunks)
        
        ## Variables de apoyo en los distintos loops
        microbatch = 0
        total_filas = 0

        precios = []
        t_insercion_ventas   = []

        for ch in df_gen:

            microbatch += 1

            logs.info(f"Transformando microbatch {microbatch}")

            df =(
                ch.pipe(add_timestamp)
                .pipe(parse_date)
                )
                
            for row in df.itertuples(index=False):
                
                rows, calendar_row = row_dict_transform(row)

                total_filas+=1
                
                precios.append(rows['price'])
                price_list = [i for i in precios if i is not None]
                precio_avg = sum(price_list)/len(price_list)
                precio_min = min(price_list)
                precio_max = max(price_list)

                plsqldb.insert(calendar_row, 'calendar')

                start_time = time.time()
                plsqldb.insert(rows, 'sales')
                insert_time = (time.time() - start_time) * 1000

                t_insercion_ventas.append(insert_time) 

                avg_time = (sum(t_insercion_ventas)/len(t_insercion_ventas))

                logs.info(
                f"""
                ----------- Pipeline Metrics -----------
                - Sales Table:
                    Total de filas: {total_filas}
                
                Columna 'price':
                Promedio: {precio_avg:.2f}
                Maximo: {precio_max}
                Minimo: {precio_min}

                Tiempo de insercion fila actual: 
                Total: {insert_time:.2f} ms
                Promedio: {avg_time:.2f} ms

                """)

            logs.info(f"Batch {microbatch} procesado")

        end_f = (time.time() - t_file) * 1000

        tl_file.append(end_f)

        avg_time = (sum(tl_file)/len(tl_file))
        max_time = max(tl_file)
        min_time = min(tl_file)

        logs.info(
        f"""
        ---- Procesamiento archivos ----
        Archivo {file} 
        Tiempo actual: {end_f:.2f} ms

        Total de archivos
        Tiempo promedio: {avg_time:.2f} ms
        Tiempo maximo: {max_time:.2f} ms
        Tiempo minimo: {min_time:.2f} ms
        """)

if __name__ == "__main__":
    USER = dbc.DB_USER
    PWD  = dbc.DB_PASSWORD
    HOST = dbc.DB_HOST
    DB   = dbc.DATABASE_NAME

    CHUNKSIZE = 4

    pathlist = list_datafile(path='./data')

    plsqldb = Database(HOST, USER, PWD, DB)

    logs = PipelineLogs("pipeline-prove", "pipeline.log").pipeline_logs()

    pipeline(pathlist, plsqldb, CHUNKSIZE, logs)