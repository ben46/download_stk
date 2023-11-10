import tushare as ts
from configparser import ConfigParser
from mysql.connector import Error
import time
from get_recent_trading_date import get_recent_trading_date

# Create ConfigParser object
config = ConfigParser()
# Read configuration items from the config.ini file
config.read('config.ini')
tushare_key = config.get('tushare', 'key')
pro = ts.pro_api(tushare_key)

recent_trading_date = get_recent_trading_date()
df = pro.daily(trade_date=str(recent_trading_date))
print(df)

from mysql_connection import make_mysql_connection

# Create a connection to the MySQL database
try:
    connection = make_mysql_connection()
    if connection.is_connected():
        cursor = connection.cursor()

        # Create the stocks table if it doesn't exist
        create_table_query = """
        CREATE TABLE IF NOT EXISTS stocks (
            id INT AUTO_INCREMENT PRIMARY KEY,
            ts_code VARCHAR(20) UNIQUE,  # Add a UNIQUE constraint to the ts_code column
            name VARCHAR(255)
        )
        """
        cursor.execute(create_table_query)
        connection.commit()

        # Initialize variables for rate limiting
        max_requests_per_minute = 60
        requests_made = 0
        start_time = time.time()

        # Loop through df's ts_code column to get namechange data and update the table
        for ts_code in df['ts_code']:
            print(ts_code)
            while True:
                try:
                    namechange_df = pro.namechange(ts_code=ts_code, fields='ts_code,name,start_date,end_date,change_reason')
                    break
                except:
                    print('An exception occurred')
                    time.sleep(60)

            # Check if there's any data in the namechange DataFrame
            if not namechange_df.empty:
                # Get the latest name change (assuming the DataFrame is sorted by start_date)
                latest_name = namechange_df.iloc[0]['name']

                # Use INSERT ... ON DUPLICATE KEY UPDATE to insert or update the row
                insert_query = """
                INSERT INTO stocks (ts_code, name) VALUES (%s, %s)
                ON DUPLICATE KEY UPDATE name = VALUES(name)
                """
                cursor.execute(insert_query, (ts_code, latest_name))
                connection.commit()

            # Rate limiting: Pause if we have reached the maximum requests per minute
            requests_made += 1
            elapsed_time = time.time() - start_time
            if requests_made >= max_requests_per_minute and elapsed_time < 60:
                sleep_time = 60 - elapsed_time
                time.sleep(sleep_time)
                start_time = time.time()
                requests_made = 0

except Error as e:
    print("Error: ", e)
finally:
    if connection.is_connected():
        cursor.close()
        connection.close()
