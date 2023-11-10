from create_sqlite_conn import create_sqlite_connection

def export_daily_csv():
    # 创建连接
    connection = create_sqlite_connection()
    cursor = connection.cursor()

    get_ts_code_query = "SELECT ts_code FROM stocks;"
    cursor.execute(get_ts_code_query)
    ts_code_results = cursor.fetchall()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    table_list = [table[0] for table in cursor.fetchall()]
    
    for ts_code in ts_code_results:
        table_name = f"daily{ts_code[0][:6]}"
        if table_name not in table_list:continue
        print(table_name)
        table_query = f"SELECT * FROM {table_name};"  # 修正 SQL 查询语句
        cursor.execute(table_query)
        table_results = cursor.fetchall()

        export_path = f"daily/{table_name}.csv"
        with open(export_path, 'w') as csv_file:
            # 假设表的列名为第一行
            column_names = [description[0] for description in cursor.description]
            csv_file.write(','.join(column_names) + '\n')
            
            for row in table_results:
                csv_file.write(','.join(map(str, row)) + '\n')

    # 关闭连接和游标
    cursor.close()
    connection.close()

if __name__ == "__main__":
    export_daily_csv()