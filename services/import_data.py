import pandas as pd
import numpy as np
import mysql.connector
import os
import gc
from mysql.connector import Error


def import_data_to_mysql(connection, cursor, file_path, filename):
    try:
        df = pd.read_csv(file_path, delimiter='|')
        df = df.replace(np.nan, None)
        print(f"\n ----------- \n{file_path} loaded successfully from file.")
        table_name = "_".join(filename.split("_")[3:-2])
        placeholders = ', '.join(['%s'] * len(df.columns))
        columns = ', '.join(df.columns)
        sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
        sum_reocrds = 0
        batch_size = int(os.getenv("BATCH_SIZE", 10000))
        # Insert data in batches
        for start in range(0, len(df), batch_size):
            batch_data = [tuple(row) for row in df[start:start+batch_size].values]
            sum_reocrds += len(batch_data)
            cursor.executemany(sql, batch_data)  # Batch insert
            connection.commit()  # Commit each batch
            print(f"{sum_reocrds} records imported successfully into the MySQL database.")
        print(f"Data imported successfully into the MySQL database.")
    except mysql.connector.Error as e:
        print(f"Error connecting to the database or inserting data: {e}")
    finally:
        del df
        gc.collect()


def bulk_import(folder_path):
    try:
        connection = mysql.connector.connect(
                host=os.getenv("HOST"),
                user=os.getenv("USER"),
                password=os.getenv("PASSWORD"),
                database=os.getenv("DATABASE")
            )
        cursor = connection.cursor()
        cursor.execute("SET FOREIGN_KEY_CHECKS = 0;")

        for filename in os.listdir(folder_path):
            import_data_to_mysql(connection, cursor, os.path.join(folder_path, filename), filename)
        
        cursor.execute("SET FOREIGN_KEY_CHECKS = 1;")
    except Error as e:
        print(f"Error: {e}")
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()
