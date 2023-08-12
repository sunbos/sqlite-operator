import sqlite3

class SQLiteOperator:
    def __init__(self, database_name, table_name) -> None:
        self.database_name = database_name
        self.table_name = table_name
        self.connection = sqlite3.connect(self.database_name)
        self.cursor = self.connection.cursor()
        self.table_info = self.cursor.execute(f"PRAGMA table_info({self.table_name});").fetchall()
    
    # 使用上下文管理器时以下操作有效，只处理提交、回滚事务，不处理关闭数据库事务
    def __enter__(self) -> None:
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        if exc_type is None:
            self.connection.commit()
        else:
            self.connection.rollback()
    
    # 避免使用 __del__ 方法来关闭数据库连接，因为该方法的调用时机是不确定的
    def close_connection(self) -> None:
        self.connection.close()
        print("数据库已关闭")
          
    def clear_database_table(self) -> None:
        self.cursor.executescript(f"DELETE FROM {self.table_name};VACUUM;")
    
    def get_column_names(self) -> list:
        column_names = [column[1] for column in self.table_info]
        return column_names
    
    def get_column_type_values(self) -> dict:
        column_types = {column[1]:column[2] for column in self.table_info}
        return column_types
    
    def get_column_not_null_values(self) -> dict:
        column_not_null = {column[1]:column[3] for column in self.table_info}
        return column_not_null

    def get_not_null_column_names(self) -> list:
        not_null_column_names = [column[1] for column in self.table_info if column[3] == 1]
        return not_null_column_names
    
    def get_column_default_values(self) -> dict:
        column_default_values = {column[1]:column[4] for column in self.table_info}
        return column_default_values
    
    def get_non_primary_key_default_values(self) -> dict:
        primary_key = self.get_primary_key()
        non_primary_key_default_values = {column[1]:column[4] for column in self.table_info if column[1] != primary_key}
        return non_primary_key_default_values 
    
    def get_primary_key(self) -> str:
        primary_key = [column[1] for column in self.table_info if column[5] == 1][0]
        return primary_key
    
    def insert_simple_row(self, column_names:tuple,column_values:tuple) -> None:
        with self.connection:            
            placeholders = ', '.join(['?' for _ in column_values])
            # INSERT INTO 表名 tuple(列名) VALUES tuple(占位符)
            # 使用参数化查询，可以防止 SQL 注入
            query = f"INSERT INTO {self.table_name} {column_names} VALUES ({placeholders})"
            self.cursor.execute(query, column_values)
    
    def insert_multiple_rows(self, column_names:tuple, row_values:list) -> None:
        with self.connection:
            placeholders = ', '.join(['?' for _ in column_names])
            query = f"INSERT INTO {self.table_name} {column_names} VALUES ({placeholders})"
            self.cursor.executemany(query, row_values)
