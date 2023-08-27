import sqlite3
import uuid

class SQLiteOperator:

    def __init__(self, database_name, table_name) -> None:
        self.database_name = database_name
        self.table_name = table_name
        self.connection = None
        self.cursor = None
        self.table_info = None
        self.insert_buffer = []
        self.buffer_limit = 100

    def __enter__(self) -> None:
        """
        上下文管理器的入口点。连接到数据库并创建游标。
        """
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """
        上下文管理器的出口点。提交或回滚事务并断开与数据库的连接。

        参数:
            exc_type: 异常的类型。
            exc_val: 异常的值。
            exc_tb: 异常的回溯信息。
        """
        if exc_type is None:
            self.connection.commit()
        else:
            self.connection.rollback()
        self.disconnect()

    def connect(self) -> None:
        """
        连接到数据库并创建游标。
        """
        self.connection = sqlite3.connect(self.database_name)
        self.cursor = self.connection.cursor()

    def disconnect(self) -> None:
        """
        断开与数据库的连接。
        """
        if self.connection:
            self.connection.close()

    def execute_with_handling(self, query, params=None) -> None:
        """
        执行带有错误处理的查询。

        参数:
            query (str): 要执行的SQL查询。
            params (tuple): 查询中要使用的参数。

        异常:
            sqlite3.Error: 如果执行查询时出错。
        """
        try:
            if params:
                self.cursor.execute(query, params)
            else:
                self.cursor.execute(query)
        except sqlite3.Error as e:
            print(f"数据库错误: {e}")
