import os
from services.notify import ms_alert
from database import get_conn, close_conn


class LoadData:

    def __init__(self) -> None:
        self.conn = get_conn()
        self.cursor = self.conn.cursor()

    def load_data(self, folder_path, filename):
        pass

    def bulk_load(self, folder_path):
        try:
            ms_alert(f"🆗[INFO] \nStart import file(s)")
            self.cursor.execute("SET FOREIGN_KEY_CHECKS = 0;")
            for filename in os.listdir(folder_path):
                sql = self.gen_script(os.path.join(folder_path, filename), filename)
                print(sql)
                self.cursor.execute(sql)
                self.conn.commit()
            self.cursor.execute("SET FOREIGN_KEY_CHECKS = 1;")
            ms_alert(f"🆗[INFO] \nCompleted import file(s) ✅")
        except Exception as e:
            print(f"Error: {e}")
            ms_alert(f"🚨[ERROR] \nError while importing data: {e}")
        finally:
            close_conn(self.conn)
    
    def get_null_cols(self, tbl_name: str):
        self.cursor.execute("""
            SELECT COLUMN_NAME
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s AND IS_NULLABLE = 'YES'
            """, (self.conn.database, tbl_name))
        return [row[0] for row in self.cursor.fetchall()]
    
    def get_all_cols(self, tbl_name: str):
        self.cursor.execute(f"""
           SHOW COLUMNS FROM {tbl_name} ;
            """)
        return [row[0] for row in self.cursor.fetchall()]
    
    def gen_script(self, folder_path, filename) -> str:
        tbl_name = "_".join(filename.split("_")[3:-2])
        all_columns = self.get_all_cols(tbl_name)
        nullable_columns = self.get_null_cols(tbl_name)
        columns_clause = ', '.join(all_columns)
        set_clause = ', '.join([f"{col} = NULLIF(@{col}, '')" for col in nullable_columns])

        # Create the LOAD DATA command
        load_data_query = f"""
            LOAD DATA LOCAL INFILE '{os.path.join(os.getcwd(), folder_path)}'
            INTO TABLE {tbl_name}
            FIELDS TERMINATED BY '|'
            LINES TERMINATED BY '\\n'
            IGNORE 1 ROWS
            SET {set_clause};
        """

        return load_data_query

