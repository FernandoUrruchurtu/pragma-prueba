from typing import Dict, Any
from sqlalchemy import create_engine, text, and_
from sqlalchemy import MetaData, Engine
from sqlalchemy import Table, Column, Integer, Date, DateTime, Float, ForeignKey, String


class Database:
    """Clase Database, donde se van a realizar todas las operaciones tanto de DML como DDL 
    """

    def __init__(self, host:str, username:str, password:str, database:str):
        self.host           = host
        self.user           = username
        self.pwd            = password
        self.db             = database
        self.engine:Engine  = None
        self.metadata       = MetaData()
        self.calendar       = None
        self.sales          = None
        self.tables         = None


    def __connection(self) -> Engine:
        """Metodo privado que crea la conexion a la base de datos de postgres

        Returns:
            Engine: retorna atributo engine con la conexion creada.
        """
        if self.engine is None:
            self.engine = create_engine(f"postgresql+psycopg2://{self.user}:{self.pwd}@{self.host}:5432/{self.db}")


        return self.engine

   
    def __create_tables(self) -> Dict[str, Table]:

        """Metodo privado que crea las tablas (DDL) usando metodos estaticos y con un ORM.

        Returns:
            Dict: retorna atributo tables con diccionario, donde las keys son tablas y los values con los atributos sales y calendar
        """

        if self.tables is None:

            self.sales = Table(
                "sales",
                self.metadata,
                Column("unique_id", Integer, primary_key=True),
                Column("user_id", Integer, nullable=False),
                Column("timestamp", Date),
                Column("price", Float),
                Column("load_date", DateTime),
                extend_existing=True
            )

            self.calendar = Table(
                "calendar",
                self.metadata,
                Column("timestamp", Date, primary_key=True),
                Column("week_day", String(20)),
                Column("day", Integer),
                Column("month", Integer),
                Column("year", Integer),
                extend_existing=True 
            )

            self.calendar.create(self.__connection(), checkfirst=True)
            self.sales.create(self.__connection(), checkfirst=True)

            self.tables = {
                "sales":self.sales,
                "calendar":self.calendar
            }

        return self.tables
    
    def __querys(self, stmt:Any):
        """Metodo privado que realiza querys sobre la base de datos

        Args:
            stmt (Any): Texto o metodo estatico

        Returns:
            Sequence: Secuencia de valores con el resultado

        """
        
        if self.engine is None:
            self.engine = self.__connection()

        with self.engine.connect() as conn:
            result = conn.execute(stmt)
            values = result.fetchall()
            conn.commit()
            #print(values)
            return values

    def __upserts(self, stmt):
        """Metodo privado para realizar update, inserts y deletes

        Args:
            stmt (Any): Puede ser o un metodo estatico o string
        """

        if self.engine is None:
            self.engine = self.__connection()
        
        with self.engine.connect() as conn:
            conn.execute(stmt)
            conn.commit()

    def select(self, query:str):
        """Metodo para realizar querys de select sobre la base de datos

        Args:
            query (str): query usando lenguaje SQL

        Returns:
            Sequence: genera una secuencia de valores
        Examples:
        >>> psqldb = Database(host, user, pwd, db)
            psqldb.select("SELECT * FROM public.sales WHERE price > 50")
        
        """

        return self.__querys(text(query))
    
    def insert(self, rows:dict, table:str):
        """Realiza setencia SQL INSERT INTO sobre la base de datos.


        Args:
            rows (dict): diccionario que debe contener {nombre_campo:valor}
            table (str): nombre de tabla a la que se le va a realizar la insercion

        Returns:
            None: None
        Examples:
        >>> psqldb = Database(host, user, pwd, db)
            rows = {"user_id":1, "price":50}
            psqldb.insert(rows, 'sales')
        
        """

        if self.tables is None:
            self.tables = self.__create_tables()

        if table in self.tables:

            if table == 'calendar':
                try:
                    stmt = self.tables[table].insert().values(rows)
                    return self.__upserts(stmt)
                except Exception:
                    print('>Table calendar already has this date')
            else:
                stmt = self.tables[table].insert().values(rows)
                return self.__upserts(stmt)
        else:
            print("Table not found")

    def update(self, row:dict, table:str): 

        """Realiza setencia update, solo sobre la tabla sales, para otras tablas se debe definir

        Raises:
            e: Crea excepcion si se va a generar update sobre un registro que no existe

        Returns:
            None: None
        """

        if self.tables is None:
            self.tables = self.__create_tables()

        if table in self.tables:

            ## Validar que los registros no existan

            if table == 'sales':
                sales = self.tables[table]
                __columns = sales.columns
                stmt = sales.select().where(
                    and_(
                        __columns.user_id == row['user_id'], 
                        __columns.timestamp == row['timestamp'], 
                        __columns.price == row['price']
                    )
                )
                values = self.__querys(stmt)
                try:
                    id = values[0][0]
                except Exception as e:
                    raise e

                if len(values)>1 and type(id) == int:
                    print('We have duplicates!')
                    stmt = sales.update().where(__columns.unique_id == id).values(row)
                    return self.__upserts(stmt)



        



        




