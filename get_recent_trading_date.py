import tushare as ts
from configparser import ConfigParser
import os
import pandas as pd
import time

# 创建ConfigParser对象
config = ConfigParser()
# 从配置文件中读取配置项
config.read('config.ini')
tushare_key = config.get('tushare', 'key')
pro = ts.pro_api(tushare_key)

# 定义缓存文件名的字典
cache_files = {
    '000001.SZ': 'cache/daily_cache_000001.csv',
    '000002.SZ': 'cache/daily_cache_000002.csv'
}

def is_cache_valid(cache_file):
    if not os.path.exists(cache_file):
        return False
    
    # 获取缓存文件的最后修改时间
    cache_mtime = os.path.getmtime(cache_file)
    current_time = time.time()
    
    # 判断是否缓存文件的修改时间在24小时内
    return (current_time - cache_mtime) < 24 * 3600  # 24小时的秒数

def get_last_day(ts_code):
    cache_file = cache_files.get(ts_code, None)
    if cache_file is None:
        return None

    if is_cache_valid(cache_file):
        # 如果调用pro.daily失败但缓存有效，则从缓存中读取数据
        df_cache = pd.read_csv(cache_file)
        return df_cache.iloc[0]['trade_date']
    else:
        try:
            # 尝试获取数据，如果成功则更新缓存文件
            df = pro.daily(ts_code=ts_code)
            trade_date = df.iloc[0]['trade_date']
            df.to_csv(cache_file, index=False)
            return trade_date
        except Exception as e:
            if os.path.exists(cache_file):
                df_cache = pd.read_csv(cache_file)
                return df_cache.iloc[0]['trade_date']
            else:
                raise e

def get_recent_trading_date():
    # 获取两只股票的最后交易日期
    trade_date_000001 = get_last_day('000001.SZ')
    trade_date_000002 = get_last_day('000002.SZ')
    if trade_date_000001 is not None and trade_date_000002 is not None:
        if trade_date_000001 >= trade_date_000002:
            return trade_date_000001
        else:
            return trade_date_000002
    else:
        raise Exception("获取最后交易日期失败")

if __name__ == "__main__":
    print(get_recent_trading_date())