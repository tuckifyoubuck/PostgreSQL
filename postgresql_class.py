import psycopg2
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL
import sys, io, os

class PSQL:
    def __init__(self, user, password, host='localhost', database='postgres', dialect='postgresql', driver='psycopg2'):
        self.params_dict = {
            'host': host,
            'database': database,
            'user': user,
            'password': password
        }
        self.url_object = URL.create(
            dialect + '+' + driver,
            username=self.params_dict['user'],
            password=self.params_dict['password'],
            host=self.params_dict['host'],
            database=self.params_dict['database']
        )
        try:
            self.engine = create_engine(self.url_object)
        except Exception as e:
            print('error: ', e)
            print('exception type: ', type(e))
            sys.exit(1)
        print('connection successful')

    def create_new_db(self, dbname):
        """
        utilizes current DB engine in order to create a new database within the same server
        :param dbname: string representing database to be created
        :return:
        """
        create_db_query = 'CREATE DATABASE '+dbname+';'
        with self.engine.begin() as connection:
            try:
                connection.execute(text('commit'))
                connection.execute(text(create_db_query))
                print(dbname, 'database created!')
            except Exception as e:
                print('error: ', e)
                print('exception type: ', type(e))
                sys.exit(1)

    def create_schemas(self, schemas):
        with self.engine.begin() as connection:
            try:
                for schema in schemas:
                    connection.execute(text(f'CREATE SCHEMA IF NOT EXISTS {schema.lower()};'))
                    print(f'{schema} schema created!')
            except Exception as e:
                print('error: ', e)
                print('exception type: ', type(e))
                sys.exit(1)

    def create_tbl(self, dataframe, tbl_name, schema, if_exists='fail'):
        try:
            # takes column names and data types and initializes empty table in DB
            dataframe.head(0).to_sql(name=tbl_name.lower(), con=self.engine, schema=schema, if_exists=if_exists)
            print(tbl_name, 'table created!')
        except Exception as e:
            print('error: ', e)
            print('exception type: ', type(e))
            sys.exit(1)

    def insert_into_tbl_both(self, dataframe, tbl_name, schema='public', filename=None):
        if filename:
            # With user given filename
            dataframe.to_csv(filename, header=False, index=True)
            stdin = open(filename, 'r', encoding='utf8')
        else:
            # With in-memory buffer
            stdin= io.StringIO()
            dataframe.to_csv(stdin, header=False, index=True)
            stdin.seek(0)
        print('Connecting to DB with psycopg2...')
        con = psycopg2.connect(**self.params_dict)
        print('Connected to DB!')
        cursor = con.cursor()
        copy_sql = 'COPY ' + schema + '.' + tbl_name + """ FROM stdin WITH CSV HEADER"""
        try:
            cursor.copy_expert(sql=copy_sql, file=stdin)
            con.commit()
            con.close()
            print('Data inserted')
        except Exception as e:
            con.close()
            print('error: ', e)
            print('exception type: ', type(e))
            sys.exit(1)




