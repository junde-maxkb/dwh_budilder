from sqlalchemy import create_engine


class DataBaseManager:
    def __init__(self, connection_string: str):
        self.engine = create_engine(connection_string)

    def connect(self):
        try:
            connection = self.engine.connect()
            print("数据库连接成功")
            return connection
        except Exception as e:
            print(f"连接到数据库时发生错误： {e}")
            return None

    def close_connection(self, connection):
        if connection:
            connection.close()
            print("数据库连接关闭")
