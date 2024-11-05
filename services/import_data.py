import pandas as pd
import numpy as np
import mysql.connector
import os
import gc
from .notify import line_alert


def import_data_to_mysql(connection, cursor, file_path, filename):
    try:
        table_name = "_".join(filename.split("_")[3:-2])
        if int(os.getenv("IS_RECONCILE", 0)):
            cursor.execute(f"SELECT COUNT(id) from {table_name};")
            before_inserted_records = cursor.fetchone()[0]
        
        df = pd.read_csv(file_path, delimiter='|')
        df = df.replace(np.nan, None)
        total_records = len(df)
        line_alert(f"🆗[INFO] \nImporting data from file {filename} \n\nTotal records = {total_records}")
        print(f"\n ----------- \n{file_path} loaded successfully from file.")
        placeholders = ', '.join(['%s'] * len(df.columns))
        columns = ', '.join(df.columns)
        sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
        commited_reocrds = 0
        batch_size = int(os.getenv("BATCH_SIZE", 10000))
        for start in range(0, total_records, batch_size):
            batch_data = [tuple(row) for row in df[start:start+batch_size].values]
            cursor.executemany(sql, batch_data)
            connection.commit()
            commited_reocrds += len(batch_data)
            print(f"{commited_reocrds} records imported successfully into the MySQL database.")
        print(f"Data imported successfully into the MySQL database.")
        
        if int(os.getenv("IS_RECONCILE", 0)):
            cursor.execute(f"SELECT COUNT(id) from {table_name};")
            inserted_records = cursor.fetchone()[0] - before_inserted_records
            if inserted_records == total_records:
                line_alert(f"🆗[INFO][RECONCILATION] \nAll record on file: {filename} has been inserted \n\nTotal records = {total_records}")
            else:
                line_alert(f"🚨[ERROR][RECONCILATION] \n{commited_reocrds} == {total_records} on file: {filename}")
    except mysql.connector.Error as e:
        print(f"Error connecting to the database or inserting data: {e}")
        line_alert(f"🚨[ERROR] \nError connecting to the database or inserting data: {e}")
    finally:
        del df
        gc.collect()


def bulk_import(folder_path):
    try:
        line_alert(f"🆗[INFO] \nStart import file(s)")
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
        line_alert(f"🆗[INFO] \nCompleted import file(s) ✅")
    except Exception as e:
        print(f"Error: {e}")
        line_alert(f"🚨[ERROR] \nError while importing data: {e}")
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()
