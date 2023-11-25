import asyncio
import tushare as ts
import aiosqlite
from configparser import ConfigParser
from get_recent_trading_date import get_recent_trading_date
from get_pro import get_pro
from table_name import *

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

async def insert_stock_data(ts_code, recent_trading_date, connection, semaphore, logger):
    async with semaphore:
        async with connection.cursor() as cursor:
            await process_and_insert_stock_data(ts_code, recent_trading_date, cursor, connection, logger)

async def process_and_insert_stock_data(ts_code, recent_trading_date, cursor, connection, logger):
    try:
        pro = get_pro()
        idx = 0
        TRY_COUNT = 3
        while idx < TRY_COUNT:
            try:
                logger.info(f"Fetching {ts_code}")
                df = ts.pro_bar(ts_code=ts_code, adj='qfq', start_date='20140101', end_date=str(recent_trading_date), api=pro)
                break
            except Exception as e:
                logger.warning(f"An exception occurred for {ts_code}: {e}")
                await asyncio.sleep(16)
                idx += 1
        
        if idx >= TRY_COUNT:
            logger.warning("Tried more than 3 times", ts_code)
            return

        logger.info(f"Inserting {ts_code} ...")
        table_name = table_name(ts_code=ts_code)
        
        _create_table_query = create_table_query(table_name=table_name)
        await cursor.execute(_create_table_query)

        # Prepare data for batch insert
        insert_data = [(row['ts_code'], row['trade_date'], row['open'], row['high'], row['low'], row['close'],
                        row['pre_close'], row['change'], row['pct_chg'], row['vol'], row['amount'])
                       for index, row in df.iterrows()]
        insert_query = insert_sql(table_name=table_name)
        await cursor.executemany(insert_query, insert_data)

        logger.info(f"Finish {ts_code}")
        await connection.commit()
    except Exception as e:
        logger.warning(f"Error processing {ts_code}: {e}")

import tqdm

# 在主函数main()中创建tqdm的进度条
async def main():
    from log import logger
    import time
    t0 = time.time()
    db_connection = await create_sqlite_connection()
    cursor = await db_connection.cursor()
    get_ts_code_query = "SELECT ts_code FROM stocks order by ts_code asc;"
    await cursor.execute(get_ts_code_query)
    ts_code_results = await cursor.fetchall()
    await cursor.close()
    # ts_code_results = ts_code_results[:30]
    recent_trading_date = get_recent_trading_date()
    logger.info(f"Recent trading date: {recent_trading_date}")
    # 使用tqdm创建进度条
    with tqdm.tqdm(total=len(ts_code_results), desc="Processing Stocks") as pbar:
        tasks = []
        semaphore = asyncio.Semaphore(3)  # 每个协程都有自己的信号量，最多同时并发10个协程
        for ts_code in ts_code_results:
            ts_code = ts_code[0]
            task = asyncio.create_task(insert_stock_data(ts_code, recent_trading_date, db_connection, semaphore, logger))
            task.add_done_callback(lambda x: pbar.update(1))  # 每完成一个任务，更新进度条
            tasks.append(task)
        await asyncio.gather(*tasks) 
    logger.info(time.time()-t0)

    
if __name__ == "__main__":
    asyncio.run(main())
