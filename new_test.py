import sqlite3
class SQLiteOperator:
    def __init__(self, database_name, table_name) -> None:
        self.database_name = database_name
        self.table_name = table_name
        self.connection = None
        self.cursor = None
        self.table_info = None
        self.insert_buffer = []
        self.buffer_limit = 100

    def connect(self):
        self.connection = sqlite3.connect(self.database_name)
        self.cursor = self.connection.cursor()

    def disconnect(self):
        if self.connection:
            self.connection.close()

    def execute_with_handling(self, query, params=None):
        try:
            if params:
                self.cursor.execute(query, params)
            else:
                self.cursor.execute(query)
        except sqlite3.Error as e:
            print(f"Database error: {e}")
    
    def clear_database_table(self):
        query = f"DELETE FROM {self.table_name}; VACUUM;"
        self.connect()
        self.execute_with_handling(query)
        self.disconnect()

    def get_table_info(self):
        if not self.table_info:
            query = f"PRAGMA table_info({self.table_name});"
            self.connect()
            self.execute_with_handling(query)
            self.table_info = self.cursor.fetchall()
            self.disconnect()
        return self.table_info

    def get_column_info(self, info_index: int) -> dict:
        return {column[1]: column[info_index] for column in self.get_table_info()}

    def get_column_names(self) -> list:
        return [column[1] for column in self.get_table_info()]

    def get_primary_key(self) -> str:
        primary_keys = [column[1] for column in self.get_table_info() if column[5] == 1]
        return primary_keys[0] if primary_keys else None

    def _insert_data(self, column_names: tuple, column_values: tuple):
        placeholders = ', '.join(['?' for _ in column_names])
        query = f"INSERT INTO {self.table_name} ({', '.join(column_names)}) VALUES ({placeholders})"
        self.execute_with_handling(query, column_values)

    def buffer_insert(self, column_names: tuple, column_values: tuple) -> None:
        self.insert_buffer.append((column_names, column_values))
        if len(self.insert_buffer) >= self.buffer_limit:
            self.execute_buffered_inserts()

    def execute_buffered_inserts(self):
        self.connect()
        for column_names, column_values in self.insert_buffer:
            self._insert_data(column_names, column_values)
        self.connection.commit()
        self.disconnect()
        self.insert_buffer = []

    def __enter__(self) -> None:
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        if exc_type is None:
            self.connection.commit()
        else:
            self.connection.rollback()
        self.disconnect()

# The above code provides the core functionalities. You can further expand the class with more specific methods.
