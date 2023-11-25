from create_sqlite_conn import create_sqlite_connection

def export_daily_csv():
    # 创建连接
    connection = create_sqlite_connection()
    cursor = connection.cursor()

    get_ts_code_query = "SELECT ts_code FROM stocks;"
    cursor.execute(get_ts_code_query)
    ts_code_results = cursor.fetchall()
    print(ts_code_results)
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    table_list = [table[0] for table in cursor.fetchall()]
    print(table_list)
    for ts_code in ts_code_results:
        table_name = f"daily{ts_code[0][:6]}"
        if table_name not in table_list:
            continue
        print(table_name)
        table_query = f"SELECT trade_date,open,high,low,close,vol FROM {table_name} order by trade_date desc;"  # 修正 SQL 查询语句
        cursor.execute(table_query)
        table_results = cursor.fetchall()

        export_path = f"daily/{table_name}.csv"
        with open(export_path, 'w') as csv_file:
            # 假设表的列名为第一行
            column_names = [description[0] for description in cursor.description]
            csv_file.write(','.join(column_names) + '\n')

            for row in table_results:
                # 对"open"、"high"、"low"、"close" 列进行小数乘法操作
                modified_row = [str(int(val * 1000)) if isinstance(val, float) else str(val) for val in row]
                
                # 将日期格式从 "yyyyMMdd" 改为 "yyyy-MM-dd"
                modified_row[0] = f"{modified_row[0][:4]}-{modified_row[0][4:6]}-{modified_row[0][6:]}"
                
                csv_file.write(','.join(modified_row) + '\n')

    # 关闭连接和游标
    cursor.close()
    connection.close()

if __name__ == "__main__":
    export_daily_csv()
