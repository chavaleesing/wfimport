import os
import gc
import gzip
import shutil
import time
import uuid

from datetime import datetime, time as dtime

import pandas as pd
from services.notify import ms_alert
from database import get_conn, close_conn

from dotenv import dotenv_values
env_vars = dotenv_values(".env")

class ImportData:

    def __init__(self) -> None:
        self.conn = get_conn()
        self.cursor = self.conn.cursor()
        self.unique_key = str(uuid.uuid4())[:8]
        self.all_counts = 0

    def get_preprocessed_file_path(self, file_path):
        list_paths = file_path.split("/")
        list_paths.insert(2, "preprocessed")
        preprocessed_file_path = os.path.join(*list_paths)
        return preprocessed_file_path

    def import_data_to_mysql(self, file_path, filename) -> None:
        try:
            preprocessed_file_path = None
            commited_reocrds = 0
            df = None
            tbl_name = "_".join(filename.split("_")[:-2])
            ms_alert(f"[INFO][{self.unique_key}] - - - Start file {filename}, table={tbl_name} - - -")
            if "preprocessed" in file_path:
                preprocessed_file_path = file_path
            else:
                self.preprocess_and_load(file_path=file_path, delimiter="|", expected_columns=self.get_count_cols(tbl_name))
                preprocessed_file_path = self.get_preprocessed_file_path(file_path)
            df = pd.read_csv(preprocessed_file_path, delimiter='|', keep_default_na=False, low_memory=False, dtype=str)
            df = df.replace('[NULL]', None)
            df = df.replace(r'\\n', '\n', regex=True)
            total_records = len(df)
            ms_alert(f"[INFO][{self.unique_key}] Importing data from file {filename}, table={tbl_name} | Total records = {total_records}")
            placeholders = ', '.join(['%s'] * len(df.columns))
            columns = ', '.join(df.columns)
            sql = f"INSERT INTO {tbl_name} ({columns}) VALUES ({placeholders})"
            batch_size = int(env_vars["BATCH_SIZE"])
            for start in range(0, total_records, batch_size):
                batch_data = [tuple(row) for row in df[start:start+batch_size].values]
                self.cursor.executemany(sql, batch_data)
                self.conn.commit()
                commited_reocrds += len(batch_data)
                self.all_counts += len(batch_data)
                print(f"[{datetime.strftime(datetime.now(), '%Y-%m-%d %H:%M:%S')}][{self.unique_key}] {commited_reocrds} records imported successfully (all_counts={self.all_counts})")
        except Exception as e:
            self.fix_error_file(preprocessed_file_path, filename, commited_reocrds)
            raise e
        finally:
            del df
            gc.collect()
            time.sleep(1)

    def fix_error_file(self, preprocessed_file_path, filename, inserted_record) -> None:
        try:
            ms_alert(f"🚨 🚨 🚨 [ERROR][{self.unique_key}] Fixing file: {filename}")
            with open(preprocessed_file_path, 'r', encoding='utf-8') as file:
                lines = file.readlines()
            lines_to_keep = lines[:1] + lines[inserted_record+1:]
            directory = os.path.dirname(preprocessed_file_path)
            edited_preprocessed_file_path = os.path.join(directory, filename.split(".")[0] + f"edit{time.strftime('%H%M%S', time.localtime())}.txt")
            with open(edited_preprocessed_file_path, 'w', encoding='utf-8') as file:
                file.writelines(lines_to_keep)
            self.remove_processed_file(preprocessed_file_path)
        except Exception as e:
            print(f"Cannot fixed file {filename} as {e}")

    def replace_empty_str(self, df, tbl_name) -> pd.DataFrame:
        notnull_cols = self.get_notnull_cols(tbl_name=tbl_name)
        for col in notnull_cols:
            df[col] = df[col].fillna("")
        return df

    def get_col_convert_col_str(self, tbl_name: str) -> dict:
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
    
    def get_notnull_cols(self, tbl_name: str) -> list:
        self.cursor.execute("""
            SELECT COLUMN_NAME
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s AND IS_NULLABLE = 'NO' AND DATA_TYPE = 'varchar'
            """, (self.conn.database, tbl_name))
        return [row[0] for row in self.cursor.fetchall()]
    
    def get_count_cols(self, tbl_name: str) -> int:
        self.cursor.execute("""
            SELECT COUNT(*) AS column_count
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
            """, (self.conn.database, tbl_name))
        col_count = self.cursor.fetchone()[0]
        return col_count
    
    def is_exceed_time(self) -> bool:
        # Validate time, If NOT between 23:00 - 03:30 => this will return True
        current_time = datetime.now().time()
        start_time = dtime(23, 0)  # 11:00 PM
        end_time = dtime(3, 30)    # 3:30 AM
        is_exceed = not(start_time <= current_time or current_time < end_time)
        return is_exceed
    
    def bulk_import(self, folder_path) -> None:
        try:
            ms_alert(f"[INFO][{self.unique_key}] ⁍ ⁍ ⁍ ⁍ ⁍ Start import file(s) on {folder_path} ⁌ ⁌ ⁌ ⁌ ⁌")
            self.cursor.execute("SET FOREIGN_KEY_CHECKS = 0;")
            processed_files = []
            for filename in os.listdir(folder_path):
                if self.is_exceed_time():
                    ms_alert(f"[INFO][{self.unique_key}] Exceed time process")
                    break
                filepath = os.path.join(folder_path, filename)
                if os.path.isfile(filepath):
                    txt_filename = filename
                    # Depress .gz
                    if filename[-3:] == ".gz":
                        txt_filename = filename[:-3]
                        with gzip.open(filepath, 'rb') as f_in:
                            with open(os.path.join(folder_path, txt_filename), 'wb') as f_out:
                                shutil.copyfileobj(f_in, f_out)
                                self.remove_processed_file(os.path.join(folder_path, filename))
                    if txt_filename not in processed_files:
                        # process file
                        file_path = os.path.join(folder_path, txt_filename)
                        self.import_data_to_mysql(file_path, txt_filename)
                        processed_files.append(txt_filename)
                        self.add_success_file(txt_filename)
                        self.remove_processed_file(self.get_preprocessed_file_path(file_path))
            
            # Rerun fixed files on path preprocessed
            pre_folder_path = folder_path + "/preprocessed"
            if os.path.exists(pre_folder_path):
                for filename in os.listdir(pre_folder_path):
                    if self.is_exceed_time():
                        ms_alert(f"[INFO][{self.unique_key}] Exceed time process")
                        break
                    txt_filename = filename
                    file_path = os.path.join(pre_folder_path, txt_filename)
                    self.import_data_to_mysql(file_path, txt_filename)
                    processed_files.append(txt_filename)
                    self.add_success_file(txt_filename)
                    self.remove_processed_file(file_path)
                           
            self.cursor.execute("SET FOREIGN_KEY_CHECKS = 1;")
            ms_alert(f"[INFO][{self.unique_key}] Completed import file(s) ✅ processed_files = {processed_files}")
        except Exception as e:
            ms_alert(f"🚨 🚨 🚨 [ERROR][{self.unique_key}] Error while importing data: {e}")
            raise e
        finally:
            close_conn(self.conn)

    def add_success_file(self, filename) -> None:
        with open('success_file_list.txt', 'a', encoding='utf-8') as file:
            file.write(f"[{self.unique_key}][{datetime.now()}] - {filename} \n")

    def remove_processed_file(self, file_path):
        try:
            os.remove(file_path)
        except Exception as e:
            print(f"Error: {e}")

    def preprocess_and_load(self, file_path, delimiter, expected_columns):
        lines = []
        current_record = ""
        prev = None
        with open(file_path, mode='r', encoding='utf-8') as file:
            i = 0
            for line in file:
                current_record = line.strip()
                if i == 0:
                    ms_alert(f"[INFO][{self.unique_key}] Preprocessing file:{file_path}, delimeter_count = {current_record.count(delimiter)}, expected_columns = {expected_columns}")
                    if current_record.count(delimiter) != expected_columns - 1:
                        ms_alert("Columns header on csv != column on DB")
                        raise Exception("Columns header on csv != Columns on DB")
                if i % 100000 == 0:
                    print(f"\rpreprocessing {i}", flush=True, end="")
                if current_record.count(delimiter) == expected_columns - 1:
                    lines.append(current_record)
                    prev= None
                else:
                    if prev:
                        if prev.count(delimiter) + current_record.count(delimiter) == expected_columns - 1:
                            lines.append(prev + current_record)
                            prev = None
                        else:
                            current_record += '\\n'
                            prev += current_record
                    else:
                        current_record += '\\n'
                        prev = current_record
                i += 1
                
        data_str = "\n".join(lines)
        preprocessed_file_path = self.get_preprocessed_file_path(file_path)
        os.makedirs(os.path.dirname(preprocessed_file_path), exist_ok=True)
        with open(preprocessed_file_path, 'w') as file:
            file.write(data_str)
        self.remove_processed_file(file_path)
        print()
