import sqlite3

class SQLiteOperator:
    def __init__(self, database_name, table_name) -> None:
        self.database_name = database_name
        self.table_name = table_name
        self.connection = sqlite3.connect(self.database_name)
        self.cursor = self.connection.cursor()
        self.table_info = self.get_table_info()
        # 初始化缓冲区
        self.insert_buffer = []
    
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
    
    # 清除表      
    def clear_database_table(self) -> None:
        self.cursor.executescript(f"DELETE FROM {self.table_name};VACUUM;")
    
    # 获取表的所有信息
    def get_table_info(self):
        self.cursor.execute(f"PRAGMA table_info({self.table_name});")
        table_info = self.cursor.fetchall()
        return table_info
    
    # 获取表指定属性的信息
    def get_column_info(self, info_index: int) -> dict:
        column_info = {column[1]: column[info_index] for column in self.table_info}
        return column_info
    
    # 获取所有列名
    def get_column_names(self) -> list:
        column_names = [column[1] for column in self.table_info]
        return column_names
    
    # 获取所有列名的类型
    def get_column_type_values(self) -> dict:
        column_types = self.get_column_info(2)
        return column_types
    
    # 获取所有列名是否为非空值
    def get_column_not_null_values(self) -> dict:
        column_not_null = self.get_column_info(3)
        return column_not_null

    # 获取非空值的列名
    def get_not_null_column_names(self) -> list:
        not_null_column_names = [column[1] for column in self.table_info if column[3] == 1]
        return not_null_column_names
    
    # 获取所有列名的默认值
    def get_column_default_values(self) -> dict:
        primary_key = self.get_primary_key()
        not_null_column_names = self.get_not_null_column_names()
        column_default_values = self.get_column_info(4)
        # 如果是主键，将其标记为"PRIMARY KEY"
        if primary_key:
            column_default_values[primary_key] = "PRIMARY KEY"
        # 如果是不为空，将其标记为"NOT NULL"
        for column_name in not_null_column_names:
            if column_name in column_default_values:
                column_default_values[column_name] = "NOT NULL"
        # 如果是空，将其标记为"NULL"；如果默认值为空，将其标记为"DEFAULT NULL"
        for column_name in column_default_values:
            if column_default_values[column_name] == None:
                column_default_values[column_name] = "NULL"
            elif column_default_values[column_name] == "NULL":
                column_default_values[column_name] = "DEFAULT NULL"
        return column_default_values
    
    # 获取除主键以外其他配置项的默认值
    def get_non_primary_key_default_values(self) -> dict:
        primary_key = self.get_primary_key()
        not_null_column_names = self.get_not_null_column_names()
        non_primary_key_default_values = {column[1]:column[4] for column in self.table_info if column[1] != primary_key}
        # 如果是不为空，将其标记为"NOT NULL"
        for column_name in not_null_column_names:
            if column_name in non_primary_key_default_values:
                non_primary_key_default_values[column_name] = "NOT NULL"
        # 如果是空，将其标记为"NULL"；如果默认值为空，将其标记为"DEFAULT NULL"
        for column_name in non_primary_key_default_values:
            if non_primary_key_default_values[column_name] == None:
                non_primary_key_default_values[column_name] = "NULL"
            elif non_primary_key_default_values[column_name] == "NULL":
                non_primary_key_default_values[column_name] = "DEFAULT NULL"        
        return non_primary_key_default_values 
    
    # 获取主键
    def get_primary_key(self) -> str:
        primary_key = [column[1] for column in self.table_info if column[5] == 1]
        return primary_key[0] if primary_key else None
    
    # 插入默认值第一种方法：先查找非主键的值，如果默认值为“NULL”或者“DEFAULT NULL”，设置成None
    def insert_default_values1(self) -> None:
        non_primary_key_default_values = self.get_non_primary_key_default_values()

        for column_name in non_primary_key_default_values:
            if non_primary_key_default_values[column_name] == "NULL":
                non_primary_key_default_values[column_name] = None
            elif non_primary_key_default_values[column_name] == "DEFAULT NULL":
                non_primary_key_default_values[column_name] = None
        
        column_names = tuple(non_primary_key_default_values.keys())
        column_values = tuple(non_primary_key_default_values.values())
        
        with self.connection:
            placeholders = ', '.join(['?' for _ in column_names])
            query = f"INSERT INTO {self.table_name} ({', '.join(column_names)}) VALUES ({placeholders})"
            self.cursor.execute(query, column_values)
    
    # 插入默认值第二种方法：先除主键其他数据提取出来，再将列名不为空的依次赋值为“NOT NULL”
    def insert_default_values2(self) -> None:
        not_null_column_names = self.get_not_null_column_names()
        not_null_values = {}
        for column_name in not_null_column_names:
            not_null_values.update({column_name: "NOT NULL"})
        
        column_names = tuple(not_null_values.keys())
        column_values = tuple(not_null_values.values())

        with self.connection:
            placeholders = ', '.join(['?' for _ in column_values])
            query = f"INSERT INTO {self.table_name} ({', '.join(column_names)}) VALUES ({placeholders})"
            self.cursor.execute(query, column_values)
            
    # 插入单行数据        
    def insert_simple_row(self, column_names:tuple,column_values:tuple) -> None:
        with self.connection:           
            placeholders = ', '.join(['?' for _ in column_values])
            query = f"INSERT INTO {self.table_name} ({', '.join(column_names)}) VALUES ({placeholders})"
            self.cursor.execute(query, column_values)
    
    # 插入多行数据
    def insert_multiple_rows(self, column_names:tuple, row_values:list) -> None:
        with self.connection:
            placeholders = ', '.join(['?' for _ in column_names])
            query = f"INSERT INTO {self.table_name} ({', '.join(column_names)}) VALUES ({placeholders})"
            self.cursor.executemany(query, row_values)

    # 可以执行多个插入语句
    # 将插入操作添加到缓冲区
    def buffer_insert(self, column_names: tuple, column_values: tuple) -> None:
        self.insert_buffer.append((column_names, column_values))
    
    # 执行缓冲区中的所有插入操作并提交
    def execute_buffered_inserts(self):
        with self.connection:
            for column_names, column_values in self.insert_buffer:
                placeholders = ', '.join(['?' for _ in column_values])
                query = f"INSERT INTO {self.table_name} ({', '.join(column_names)}) VALUES ({placeholders})"
                self.cursor.execute(query, column_values)
            self.insert_buffer = []  # 清空缓冲区   
