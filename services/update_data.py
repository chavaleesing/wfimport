import os
import gc
import gzip
import shutil
import time
import uuid
import json

from datetime import datetime

import pandas as pd
from services.notify import ms_alert
from database import get_conn, close_conn
from services.helper import Helper

from dotenv import dotenv_values
env_vars = dotenv_values(".env")

class UpdateData:

    def __init__(self) -> None:
        self.conn = get_conn()
        self.cursor = self.conn.cursor()
        self.unique_key = str(uuid.uuid4())[:8]
        self.all_counts = 0

    def update_data_to_mysql(self, file_path, filename) -> None:
        try:
            preprocessed_file_path = None
            commited_reocrds = 0
            df = None
            tbl_name = "_".join(filename.split("_")[:-2]) + "_temp"
            ms_alert(f"[INFO][{self.unique_key}] - - - Start file {filename}, table={tbl_name} - - -")
            if "preprocessed" in file_path:
                preprocessed_file_path = file_path
            else:
                Helper.preprocess_and_load(unique_key=self.unique_key, file_path=file_path, delimiter="|", expected_columns=Helper.get_count_cols(self.conn, self.cursor, tbl_name))
                preprocessed_file_path = Helper.get_preprocessed_file_path(file_path)
            df = pd.read_csv(preprocessed_file_path, delimiter='|', keep_default_na=False, low_memory=False, dtype=str)
            df = df.replace('[NULL]', None)
            df = df.replace(r'\\n', '\n', regex=True)
            total_records = len(df)
            ms_alert(f"[INFO][{self.unique_key}] Updating data from file {filename}, table={tbl_name} | Total records = {total_records}")

            identifier_column = "id"
            columns = [col for col in df.columns if col != identifier_column and (col.startswith("encrypted_") or col.startswith("hashed_"))]
            set_clause = ', '.join([f"{col} = %s" for col in columns])
            sql = f"UPDATE {tbl_name} SET {set_clause} WHERE {identifier_column} = %s"
            if env_vars.get("UPDATE_CONDITION_MAPPING"):
                update_condition_mapping = json.loads(env_vars.get("UPDATE_CONDITION_MAPPING"))
                tbl_main = "_".join(filename.split("_")[:-2])
                col_cond = update_condition_mapping.get(tbl_main)
                sql += f" and {col_cond} is NULL"

            batch_size = int(env_vars["BATCH_SIZE"])
            batch_size = 3
            for start in range(0, total_records, batch_size):
                batch_data = [
                    tuple(row[col] for col in columns) + (row[identifier_column],)
                    for _, row in df[start:start+batch_size].iterrows()
                ]
                print("------")
                print(sql)
                print("------")
                print(batch_data)
                print("------")

                self.cursor.executemany(sql, batch_data)
                self.conn.commit()
                commited_reocrds += len(batch_data)
                self.all_counts += len(batch_data)
                print(f"[{datetime.strftime(datetime.now(), '%Y-%m-%d %H:%M:%S')}][{self.unique_key}] {commited_reocrds} records updated successfully (all_counts={self.all_counts})")
        except Exception as e:
            Helper.fix_error_file(self.unique_key, preprocessed_file_path, filename, commited_reocrds)
            raise e
        finally:
            del df
            gc.collect()
            time.sleep(1)

    
    def bulk_update(self, folder_path) -> None:
        try:
            ms_alert(f"[INFO][{self.unique_key}] ⁍ ⁍ ⁍ ⁍ ⁍ Start update file(s) on {folder_path} ⁌ ⁌ ⁌ ⁌ ⁌")
            self.cursor.execute("SET FOREIGN_KEY_CHECKS = 0;")
            processed_files = []
            for filename in os.listdir(folder_path):
                if Helper.is_exceed_time():
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
                                Helper.remove_processed_file(os.path.join(folder_path, filename))
                    if txt_filename not in processed_files and txt_filename[-4:] == ".txt":
                        # process file
                        file_path = os.path.join(folder_path, txt_filename)
                        self.update_data_to_mysql(file_path, txt_filename)
                        processed_files.append(txt_filename)
                        Helper.add_success_file(self.unique_key, txt_filename)
                        Helper.remove_processed_file(Helper.get_preprocessed_file_path(file_path))
                    else:
                        print(f"Pass process file: {txt_filename}")
            
            # Rerun fixed files on path preprocessed
            pre_folder_path = folder_path + "/preprocessed"
            if os.path.exists(pre_folder_path):
                for filename in os.listdir(pre_folder_path):
                    if Helper.is_exceed_time():
                        ms_alert(f"[INFO][{self.unique_key}] Exceed time process")
                        break
                    txt_filename = filename
                    file_path = os.path.join(pre_folder_path, txt_filename)
                    self.update_data_to_mysql(file_path, txt_filename)
                    processed_files.append(txt_filename)
                    Helper.add_success_file(self.unique_key, txt_filename)
                    Helper.remove_processed_file(file_path)
                           
            self.cursor.execute("SET FOREIGN_KEY_CHECKS = 1;")
            ms_alert(f"[INFO][{self.unique_key}] Completed update file(s) ✅ processed_files = {processed_files}")
        except Exception as e:
            ms_alert(f"🚨 🚨 🚨 [ERROR][{self.unique_key}] Error while importing data: {e}")
            raise e
        finally:
            close_conn(self.conn)
