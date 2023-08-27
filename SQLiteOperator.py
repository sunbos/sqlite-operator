import sqlite3
import uuid

class SQLiteOperator:
    """
    用于与 SQLite 数据库交互的类。

    属性:
        database_name (str): SQLite 数据库的名称。
        table_name (str): 数据库中的表名称。
        connection (sqlite3.Connection): 数据库连接。
        cursor (sqlite3.Cursor): 执行查询的游标。
        table_info (list): 表列的信息。
        insert_buffer (list): 批量插入的缓冲区。
        buffer_limit (int): 插入缓冲区的最大记录数。
    """

    def __init__(self, database_name, table_name) -> None:
        """
        初始化 SQLiteOperator。

        参数:
            database_name (str): SQLite 数据库的名称。
            table_name (str): 数据库中的表名称。
        """
        self.database_name = database_name
        self.table_name = table_name
        self.connection = None
        self.cursor = None
        self.table_info = None
        self.insert_buffer = []
        self.buffer_limit = 100

    def __enter__(self) -> 'SQLiteOperator':
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
            query (str): 要执行的 SQL 查询。
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

    def clear_database_table(self):
        """
        清除表中的数据内容。
        """
        sql_script = f"DELETE FROM {self.table_name}; VACUUM;"
        self.connection = sqlite3.connect(self.database_name)
        self.connection.executescript(sql_script)
        self.disconnect()

    def get_table_info(self) -> list:
        """
        获取表的所有列信息。

        返回值:
            list: 包含列信息的列表，每个元素是一个包含列属性的元组。
        """
        if not self.table_info:
            self.connect()
            self.execute_with_handling(f"SELECT * FROM pragma_table_info('{self.table_name}');")
            self.table_info = self.cursor.fetchall()               
            self.disconnect()
        return self.table_info

    def get_table_column_info(self, info_index: int = None) -> dict:
        """
        获取具体表信息中行信息的数据：cid、name、type、notnull、dflt_value、pk。
        
        参数:
            info_index (int): 要获取的特定信息的索引。

        返回值:
            dict: 包含列信息的字典，其中列名作为键，指定的信息作为值。
        """
        if info_index is None:
            raise TypeError("在调用 SQLiteOperator.get_table_column_info() 时缺少必需的位置参数 'info_index'。")
        if info_index > 5:
            raise IndexError("在调用 SQLiteOperator.get_table_column_info() 时，'info_index' 应在 0~5 的范围内。")
        table_column_info = {column[1]: column[info_index] for column in self.get_table_info()}
        return table_column_info
