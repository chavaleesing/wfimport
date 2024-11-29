import os
from mysql.connector import pooling, Error


from dotenv import dotenv_values
env_vars = dotenv_values(".env")

def get_conn():
    try:
        pool = pooling.MySQLConnectionPool(
            pool_name="poolwf",
            pool_size=5,
            host=env_vars["DB_HOST"],
            user=env_vars["DB_USER"],
            password=env_vars["DB_PASSWORD"],
            database=env_vars["DATABASE"]
        )
        connection = pool.get_connection()
        print("Database connection established.")
        return connection
    except Error as e:
        print(f"DB Error: {e}")
        raise e
    except Exception as e:
        print(f"Error: {e}")
        raise e

def close_conn(connection):
    try:
        if connection and connection.is_connected():
            connection.close()
            print("Database connection closed.")
    except Exception as e:
        print(f"Error: {e}")
        raise e
