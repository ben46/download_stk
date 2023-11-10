from create_sqlite_conn import create_sqlite_connection

def show_all_table_names():
    # 创建连接
    connection = create_sqlite_connection()
    cursor = connection.cursor()

    # 获取所有表格名称
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    table_names = cursor.fetchall()

    # 打印表格名称
    print("数据库中的表格名称:")
    for table_name in table_names:
        print(table_name[0])

    # 关闭连接和游标
    cursor.close()
    connection.close()

if __name__ == "__main__":
    show_all_table_names()
