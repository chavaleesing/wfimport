import pandas as pd
import mysql.connector
import os
import gc
from services.notify import ms_alert
from database import get_conn, close_conn
from io import StringIO


class ImportData:

    def __init__(self) -> None:
        self.conn = get_conn()
        self.cursor = self.conn.cursor()

    def import_data_to_mysql(self, file_path, filename):
        try:
            df = self.preprocess_and_load(file_path)
            tbl_name = os.getenv("TABLE_NAME", None) or "_".join(filename.split("_")[3:-2])
            if int(os.getenv("IS_RECONCILE", 0)):
                before_inserted_records = self.get_count_records(tbl_name)
            df = df.replace('[NULL]', None)
            df = df.replace(r'\\n', '\n', regex=True)
            total_records = len(df)
            ms_alert(f"🆗[INFO] \nImporting data from file {filename} \n\nTotal records = {total_records}")
            print(f"\n ----------- \n{file_path} loaded successfully from file.")
            placeholders = ', '.join(['%s'] * len(df.columns))
            columns = ', '.join(df.columns)
            sql = f"INSERT INTO {tbl_name} ({columns}) VALUES ({placeholders})"
            commited_reocrds = 0
            batch_size = int(os.getenv("BATCH_SIZE", 10000))
            
            for start in range(0, total_records, batch_size):
                batch_data = [tuple(row) for row in df[start:start+batch_size].values]
                self.cursor.executemany(sql, batch_data)
                self.conn.commit()
                commited_reocrds += len(batch_data)
                print(f"{commited_reocrds} records imported successfully into the MySQL database.")
            print(f"Data imported successfully into the MySQL database.")
            
            if int(os.getenv("IS_RECONCILE", 0)):
                count_all_records = self.get_count_records(tbl_name)
                if count_all_records == total_records + before_inserted_records:
                    ms_alert(f"🆗[INFO][RECONCILATION] \nAll record on file: {filename} has been inserted \n\nTotal records = {total_records}")
                else:
                    ms_alert(f"🚨[ERROR][RECONCILATION] \n{count_all_records} != {total_records} + {before_inserted_records} on file: {filename}")
        except mysql.connector.Error as e:
            print(f"Error connecting to the database or inserting data: {e}")
            ms_alert(f"🚨[ERROR] \nError connecting to the database or inserting data: {e}")
        finally:
            del df
            gc.collect()

    def replace_empty_str(self, df, tbl_name):
        notnull_cols = self.get_notnull_cols(tbl_name=tbl_name)
        for col in notnull_cols:
            df[col] = df[col].fillna("")
        return df

    def get_col_convert_col_str(self, tbl_name: str):
        mapping = {
            "tbl_customers": ["tn_auth_flag"],
            "tbl_customers_audit": ["tn_auth_flag"],
            "tbl_customer_address": ["postal_code"]
        }
        if mapping.get(tbl_name):
            return {i: str for i in mapping[tbl_name]}
        return {}
    
    def get_count_records(self, tbl_name: str) -> int:
        self.cursor.execute(f"SELECT COUNT(id) from {tbl_name};")
        result = self.cursor.fetchone()
        if result is not None:
            return result[0]
        return 0
    
    def get_notnull_cols(self, tbl_name: str):
        self.cursor.execute("""
            SELECT COLUMN_NAME
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s AND IS_NULLABLE = 'NO' AND DATA_TYPE = 'varchar'
            """, (self.conn.database, tbl_name))
        return [row[0] for row in self.cursor.fetchall()]
    
    def bulk_import(self, folder_path):
        try:
            ms_alert(f"🆗[INFO] \nStart import file(s)")
            self.cursor.execute("SET FOREIGN_KEY_CHECKS = 0;")
            for filename in os.listdir(folder_path):
                self.import_data_to_mysql(os.path.join(folder_path, filename), filename)
            self.cursor.execute("SET FOREIGN_KEY_CHECKS = 1;")
            ms_alert(f"🆗[INFO] \nCompleted import file(s) ✅")
        except Exception as e:
            print(f"Error: {e}")
            ms_alert(f"🚨[ERROR] \nError while importing data: {e}")
        finally:
            close_conn(self.conn)
    
    def preprocess_and_load(self, file_path, delimiter='|', expected_columns=54):
        lines = []
        current_record = ""

        with open(file_path, mode='r', encoding='utf-8') as file:
            for line in file:
                current_record = line.strip()
                if current_record.count(delimiter) == expected_columns - 1:
                    lines.append(current_record)
                    prev= None
                else:
                    if prev:
                        lines.append(prev + current_record)
                        prev = None
                    else:
                        current_record += '\\n'
                        prev = current_record
        
        data_str = "\n".join(lines)
        df = pd.read_csv(StringIO(data_str), delimiter='|', keep_default_na=False)
        return df

