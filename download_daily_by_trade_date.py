from datetime import datetime, timedelta
from get_pro import get_pro
from table_name import *
from create_sqlite_conn import create_sqlite_connection
import time
from log import logger

def download_daily_by_trade_date(trade_date, connection):
    df = pro.daily(**{
        "ts_code": "",
        "trade_date": trade_date,
        "start_date": "",
        "end_date": "",
        "offset": "",
        "limit": ""
    }, fields=[
        "ts_code",
        "trade_date",
        "open",
        "high",
        "low",
        "close",
        "pre_close",
        "change",
        "pct_chg",
        "vol",
        "amount"
    ])
    logger.info(str(df))

    # 遍历数据框架的行
    for index, row in df.iterrows():
        _table_name = table_name(row['ts_code'])  # 获取数据表名称
        _create_table_query = create_table_query(_table_name)  # 创建数据表的查询语句

        cursor = connection.cursor()  # 获取数据库连接的游标
        cursor.execute(_create_table_query)  # 执行创建数据表的查询语句
        connection.commit()  # 提交执行结果到数据库
        
        _insert_sql = insert_sql(_table_name)  # 获取插入数据的 SQL 查询语句
        _insert_data = (row['ts_code'], row['trade_date'], row['open'], row['high'], row['low'], row['close'],
                            row['pre_close'], row['change'], row['pct_chg'], row['vol'], row['amount'])
        cursor.execute(_insert_sql, _insert_data)  # 执行插入数据的 SQL 查询语句
        connection.commit()  # 提交执行结果到数据库

pro = get_pro()  # 获取数据接口实例
connection = create_sqlite_connection()  # 创建 SQLite 数据库连接

# 获取当前日期
current_date = datetime.now()

# 循环执行最近 30 天的日期
for i in range(7):
    # 计算目标日期
    target_date = current_date - timedelta(days=i)
    if target_date.weekday() >= 5:  # 5代表星期六，6代表星期日
        continue
    # 将目标日期转换为指定格式的字符串
    formatted_date = target_date.strftime("%Y%m%d")

    try:
        download_daily_by_trade_date(str(formatted_date), connection)
    except Exception as e:
        print(f'发生异常：{formatted_date}, {e}')  # 打印异常信息
        time.sleep(2)  # 等待 2 秒

connection.close()  # 关闭数据库连接
