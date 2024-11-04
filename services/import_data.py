import pandas as pd
import mysql.connector
import os


def import_data_to_mysql(file_path, filename):
    df = pd.read_csv(file_path, delimiter='|')
    df = df.where(pd.notnull(df), None)
    print(f"\n ----------- \n{file_path} loaded successfully from file.")
    try:
        table_name = "_".join(filename.split("_")[3:-2])
        connection = mysql.connector.connect(
            host=os.getenv("HOST"),
            user=os.getenv("USER"),
            password=os.getenv("PASSWORD"),
            database=os.getenv("DATABASE")
        )
        cursor = connection.cursor()
        # Prepare the SQL INSERT statement with placeholders
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
        cursor.close()
        connection.close()


def bulk_import(folder_path):
    for filename in os.listdir(folder_path):
        import_data_to_mysql(os.path.join(folder_path, filename), filename)
