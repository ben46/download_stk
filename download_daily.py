import asyncio
import tushare as ts
import aiosqlite
from configparser import ConfigParser
from get_recent_trading_date import get_recent_trading_date
from get_pro import get_pro

# 创建全局连接池
async def create_sqlite_connection():
    # 创建ConfigParser对象
    config = ConfigParser()
    # 从配置文件中读取配置项
    config.read('config.ini')
    # 获取数据库连接信息
    db_path = config.get('sqlite', 'db')
    
    connection = await aiosqlite.connect(f"{db_path}.db")
    return connection

async def insert_stock_data(ts_code, recent_trading_date, connection, semaphore):
    async with semaphore:
        async with connection.cursor() as cursor:
            await process_and_insert_stock_data(ts_code, recent_trading_date, cursor, connection)

async def process_and_insert_stock_data(ts_code, recent_trading_date, cursor, connection):
    try:
        pro = get_pro()
        idx = 0
        TRY_COUNT = 3
        while idx < TRY_COUNT:
            try:
                print(f"fetching {ts_code}")
                df = ts.pro_bar(ts_code=ts_code, adj='qfq', start_date='20180101', end_date=str(recent_trading_date), api=pro)
                break
            except Exception as e:
                print(f"An exception occurred for {ts_code}: {e}")
                await asyncio.sleep(16)
                idx += 1
        
        if idx >= TRY_COUNT:
            return

        print(f"Inserting {ts_code} ...")
        table_name = f'daily{ts_code}'
        table_name = table_name.split(".")[0]
        
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts_code TEXT,
            trade_date TEXT,
            open REAL,
            high REAL,
            low REAL,
            close REAL,
            pre_close REAL,
            `change` REAL,
            pct_chg REAL,
            vol REAL,
            amount REAL,
            UNIQUE (trade_date)
        );
        """
        await cursor.execute(create_table_query)

        # Prepare data for batch insert
        insert_data = [(row['ts_code'], row['trade_date'], row['open'], row['high'], row['low'], row['close'],
                        row['pre_close'], row['change'], row['pct_chg'], row['vol'], row['amount'])
                       for index, row in df.iterrows()]

        # Batch insert using parameterized query
        insert_query = f"INSERT INTO {table_name} (ts_code, trade_date, open, high, low, close, pre_close, `change`, pct_chg, vol, amount) " \
                        f"VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) " \
                        f"ON CONFLICT(trade_date) DO UPDATE SET open=excluded.open, high=excluded.high, " \
                        f"low=excluded.low, close=excluded.close, pre_close=excluded.pre_close, " \
                        f"`change`=excluded.`change`, pct_chg=excluded.pct_chg, vol=excluded.vol, amount=excluded.amount"
        await cursor.executemany(insert_query, insert_data)

        print(f"Finish {ts_code}")
        await connection.commit()
    except Exception as e:
        print(f"Error processing {ts_code}: {e}")

async def main():
    import time
    t0 = time.time()
    db_connection = await create_sqlite_connection()
    cursor = await db_connection.cursor()
    get_ts_code_query = "SELECT ts_code FROM stocks;"
    await cursor.execute(get_ts_code_query)
    ts_code_results = await cursor.fetchall()
    await cursor.close()
    # ts_code_results = ts_code_results[:30]
    recent_trading_date = get_recent_trading_date()
    tasks = []
    semaphore = asyncio.Semaphore(3)  # 每个协程都有自己的信号量，最多同时并发10个协程
    for ts_code in ts_code_results:
        ts_code = ts_code[0]
        task = asyncio.create_task(insert_stock_data(ts_code, recent_trading_date, db_connection, semaphore))
        tasks.append(task)
    await asyncio.gather(*tasks) 
    print(time.time()-t0)
    
if __name__ == "__main__":
    asyncio.run(main())
