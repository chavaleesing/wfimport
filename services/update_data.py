import gc
import time
import uuid
import json

from services.notify import ms_alert
from database import get_conn
from services.helper import Helper

from dotenv import dotenv_values
env_vars = dotenv_values(".env")

class UpdateData:

    def __init__(self) -> None:
        self.conn = get_conn()
        self.cursor = self.conn.cursor()
        self.unique_key = str(uuid.uuid4())[:8]
        self.all_counts = 0

    def update_data_to_mysql(self) -> None:
        try:
            tbl_mains = env_vars["UPDATE_TABLES"].split(",")
            for tbl_main in tbl_mains:
                if Helper.is_exceed_time():
                    break
                tbl_name = tbl_main +   "_temp"
                if env_vars.get("UPDATE_CONDITION_MAPPING"):
                    update_condition_mapping = json.loads(env_vars.get("UPDATE_CONDITION_MAPPING"))
                    col_cond = f" {update_condition_mapping.get(tbl_main)} is NULL"
                identifier_column = "id"
                all_cols = Helper.get_cols_name(self.conn, self.cursor, tbl_name)
                columns = [col for col in all_cols if col != identifier_column and (col.startswith("encrypted_") or col.startswith("hashed_"))]
                set_clause = ', '.join([f"main.{col} = temp.{col}" for col in columns])
                batch_size = int(env_vars["BATCH_SIZE"])
                is_process_ids = True
                while is_process_ids:
                    if Helper.is_exceed_time():
                        break
                    sql_ids = f"SELECT id FROM {tbl_main} WHERE {col_cond} limit {batch_size}"
                    self.cursor.execute(sql_ids)
                    ids = self.cursor.fetchall()
                    id_list = [id_tuple[0] for id_tuple in ids]
                    if not id_list:
                        is_process_ids = False
                        break
                    id_str = ', '.join([f"'{str(id)}'" for id in id_list])
                    sql = f"UPDATE {tbl_main} as main JOIN {tbl_name} as temp ON main.id = temp.id SET {set_clause} WHERE main.id IN ({id_str});"
                    self.cursor.execute(sql)
                    updated_rows = self.cursor.rowcount
                    self.conn.commit()
                    self.all_counts += updated_rows
                    ms_alert(f"[INFO][{self.unique_key}] Updating data table={tbl_main} | updated_records={self.all_counts}")
        except Exception as e:
            raise e
        finally:
            gc.collect()
            time.sleep(1)
