from execute_function import execute_query


def update_price_history():
    q1 = '''INSERT INTO daily_all_token_price_in_usd
        (token, timestamp_column, logindex, price_usd, transaction_date)
    SELECT
        token,
        timestamp_column,
        logindex,
        price_usd,
        transaction_date
    FROM (
        SELECT
            token,
            timestamp_column,
            CAST(logindex AS INT),
            price_usd,
            transaction_date,
            ROW_NUMBER() OVER (PARTITION BY token ORDER BY timestamp_column DESC, logindex DESC) AS rn
        FROM token_price_daily_level
    ) subquery1
    WHERE rn = 1;'''

    q2 = '''TRUNCATE TABLE token_price_daily_level;
'''
    execute_query(q1)
    execute_query(q2)


if __name__ == '__main__':
    update_price_history()
