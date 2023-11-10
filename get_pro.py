from configparser import ConfigParser
import tushare as ts
def get_pro():
    # 创建ConfigParser对象
    config = ConfigParser()
    # 从配置文件中读取配置项
    config.read('config.ini')
    # 获取数据库连接信息
    tushare_key = config.get('tushare', 'key')
    pro = ts.pro_api(tushare_key)
    return pro