from configparser import ConfigParser  # 添加对ConfigParser的引用
import sqlite3

# 创建全局连接池
def create_sqlite_connection():
    # 创建ConfigParser对象
    config = ConfigParser()
    # 从配置文件中读取配置项
    config.read('config.ini')
    # 获取数据库连接信息
    db_path = config.get('sqlite', 'db')
    connection = sqlite3.connect(f"{db_path}.db")
    return connection
