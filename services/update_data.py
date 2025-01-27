import uuid
import json

from services.notify import ms_alert
from database import get_conn, close_conn
from services.helper import Helper

from datetime import datetime

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
            if Helper.is_exceed_time(self.unique_key):
                return
            tbl_mains = env_vars["UPDATE_TABLES"].split(",")
            completed_tbl = set()
            for tbl_main in tbl_mains:
                ms_alert(f"[INFO][{self.unique_key}] Updating data table: {tbl_main}")
                if Helper.is_exceed_time(self.unique_key):
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
                    if Helper.is_exceed_time(self.unique_key):
                        break
                    sql_ids = f"SELECT id FROM {tbl_main} WHERE {col_cond} limit 1000000;"
                    self.cursor.execute(sql_ids)
                    ids = self.cursor.fetchall()
                    total_records = len(ids)
                    print(f"[{datetime.strftime(datetime.now(), '%Y-%m-%d %H:%M:%S')}][INFO][{self.unique_key}] total_records {total_records}")
                    if total_records <= 0:
                        is_process_ids = False
                        break
                    update_count = 0
                    for _, start in enumerate(range(0, total_records, batch_size)):
                        id_to_update = [id_tuple[0] for id_tuple in ids[start:start+batch_size]]
                        if not id_to_update:
                            break
                        id_str_to_update = ', '.join([f"'{str(id)}'" for id in id_to_update])
                        sql = f"UPDATE LOW_PRIORITY {tbl_main} as main JOIN {tbl_name} as temp ON main.id = temp.id SET {set_clause} WHERE main.id IN ({id_str_to_update});"
                        self.cursor.execute(sql)
                        updated_rows = self.cursor.rowcount
                        update_count += updated_rows
                        self.all_counts += updated_rows
                        print(f"[{datetime.strftime(datetime.now(), '%Y-%m-%d %H:%M:%S')}][INFO][{self.unique_key}] Updating {updated_rows} on {tbl_main} | all_counts={self.all_counts}")
                    if update_count == 0:
                        is_process_ids = False
                        break
                    else:
                        self.conn.commit()
                        print(f"[{datetime.strftime(datetime.now(), '%Y-%m-%d %H:%M:%S')}][INFO][{self.unique_key}] Updating data table={tbl_main} | updated_records={self.all_counts}")
                        completed_tbl.add(tbl_main)
            ms_alert(f"[INFO][{self.unique_key}] Completed update data completed_tbl={completed_tbl} | updated_records={self.all_counts}")
        except Exception as e:
            raise e
        finally:
            close_conn(self.conn)
