import os
import time
from datetime import datetime, time as dtime

from services.notify import ms_alert

from dotenv import dotenv_values
env_vars = dotenv_values(".env")

class Helper:

    @staticmethod
    def get_preprocessed_file_path(file_path):
        list_paths = file_path.split("/")
        list_paths.insert(2, "preprocessed")
        preprocessed_file_path = os.path.join(*list_paths)
        return preprocessed_file_path 
    
    @staticmethod
    def remove_processed_file(file_path):
        try:
            os.remove(file_path)
        except Exception as e:
            print(f"Error: {e}")

    @staticmethod
    def is_exceed_time() -> bool:
        if int(env_vars["IS_VALIDATE_TIME"]):
            # Validate time, If NOT between 23:00 - 03:30 => this will return True
            current_time = datetime.now().time()
            print(f"current time to validate: {current_time}")
            start_time = dtime(23, 0)  # 11:00 PM
            end_time = dtime(3, 30)    # 3:30 AM
            is_exceed = not(start_time <= current_time or current_time < end_time)
            return is_exceed
    
    @staticmethod
    def fix_error_file(unique_key, preprocessed_file_path, filename, inserted_record) -> None:
        try:
            ms_alert(f"🚨 🚨 🚨 [ERROR][{unique_key}] Fixing file: {filename}")
            with open(preprocessed_file_path, 'r', encoding='utf-8') as file:
                lines = file.readlines()
            lines_to_keep = lines[:1] + lines[inserted_record+1:]
            directory = os.path.dirname(preprocessed_file_path)
            edited_preprocessed_file_path = os.path.join(directory, filename.split(".")[0] + f"edit{time.strftime('%H%M%S', time.localtime())}.txt")
            with open(edited_preprocessed_file_path, 'w', encoding='utf-8') as file:
                file.writelines(lines_to_keep)
            Helper.remove_processed_file(preprocessed_file_path)
        except Exception as e:
            print(f"Cannot fixed file {filename} as {e}")

    @staticmethod
    def add_success_file(unique_key, filename) -> None:
        with open('success_file_list.txt', 'a', encoding='utf-8') as file:
            file.write(f"[{unique_key}][{datetime.now()}] - {filename} \n")


    @staticmethod
    def get_count_cols(conn, cursor, tbl_name: str) -> int:
        cursor.execute("""
            SELECT COUNT(*) AS column_count
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
            """, (conn.database, tbl_name))
        col_count = cursor.fetchone()[0]
        return col_count
    
    @staticmethod
    def get_cols_name(conn, cursor, tbl_name: str) -> list:
        cursor.execute("""
            SELECT COLUMN_NAME AS column_count
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
            """, (conn.database, tbl_name))
        cols_name = cursor.fetchall()
        return [row[0] for row in cols_name]
    
    @staticmethod
    def preprocess_and_load(unique_key, file_path, delimiter, expected_columns):
        lines = []
        current_record = ""
        prev = None
        with open(file_path, mode='r', encoding='utf-8') as file:
            i = 0
            for line in file:
                current_record = line.strip()
                if i == 0:
                    ms_alert(f"[INFO][{unique_key}] Preprocessing file:{file_path}, delimeter_count = {current_record.count(delimiter)}, expected_columns = {expected_columns}")
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
        preprocessed_file_path = Helper.get_preprocessed_file_path(file_path)
        os.makedirs(os.path.dirname(preprocessed_file_path), exist_ok=True)
        with open(preprocessed_file_path, 'w') as file:
            file.write(data_str)
        Helper.remove_processed_file(file_path)
        print()
