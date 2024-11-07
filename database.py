import os
import mysql.connector
from mysql.connector import Error


def get_conn():
    try:
        connection = mysql.connector.connect(
            host=os.getenv("HOST"),
            user=os.getenv("USER"),
            password=os.getenv("PASSWORD"),
            database=os.getenv("DATABASE")
        )
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
