import os
import dotenv

from .table_model import Table, TableName


class Database:
    def __init__(self):
        dotenv.load_dotenv(override=True)
        self.base_path = os.getenv("DATABASE_PATH")


    def is_connected(self) -> bool:
        return self.base_path is not None and os.path.exists(self.base_path)
    

    def __run_connection_test(self) -> bool:
        if not self.is_connected():
            raise ConnectionError("Database connection failed: Base path is not set or does not exist.")
        return True


    def get_table_path(self, table_name: TableName) -> str:
        self.__run_connection_test()
        return f"{self.base_path}/{table_name.value}"
    

    def table_exists(self, table_name: TableName) -> bool:
        table_path = self.get_table_path(table_name)
        return os.path.exists(table_path)
    

    def get_table(self, table_name: TableName) -> Table:
        if not self.table_exists(table_name):
            raise FileNotFoundError(f"Table '{table_name.value}' does not exist in the database.")
        return Table(name=table_name.value, base_path=self.get_table_path(table_name))

    