def table_name(ts_code):
    
    table_name = f'daily{ts_code}'
    table_name = table_name.split(".")[0]
    return table_name


def insert_sql(table_name):
    # Batch insert using parameterized query
    insert_query = f"INSERT INTO {table_name} (ts_code, trade_date, open, high, low, close, pre_close, `change`, pct_chg, vol, amount) " \
                    f"VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) " \
                    f"ON CONFLICT(trade_date) DO UPDATE SET open=excluded.open, high=excluded.high, " \
                    f"low=excluded.low, close=excluded.close, pre_close=excluded.pre_close, " \
                    f"`change`=excluded.`change`, pct_chg=excluded.pct_chg, vol=excluded.vol, amount=excluded.amount"
    return insert_query


def create_table_query(table_name):
    _create_table_query = f"""
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
    return _create_table_query

        