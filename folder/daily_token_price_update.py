import logging
from execute_function import execute_query


def update_daily_price():
    logging.basicConfig(filename='logfile.log', level=logging.DEBUG,
                        format='%(asctime)s %(levelname)s: %(message)s')

    logging.debug('Executing query 1')
    q1 = '''INSERT INTO token_price_daily_level (token, timestamp_column, logindex, price_usd, transaction_date)
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
            logindex,
            price_usd,
            utc_date as transaction_date,
            ROW_NUMBER() OVER (PARTITION BY token, transaction_date ORDER BY timestamp_column DESC, logindex DESC) AS rn
        FROM (
            SELECT
                token0_id AS token,
                timestamp AS timestamp_column,
                CAST(logindex AS INT),
                token0_outbalance AS token_outbalance,
                token0_inbalance AS token_inbalance,
                txn_amount_usd,
                DATE_TRUNC('hour', DATEADD(second, CAST(timestamp AS INT), '1970-01-01')) + INTERVAL '30 minutes' AS utc_date,
                CASE
                    WHEN token0_inbalance + token0_outbalance = 0 THEN NULL
                    ELSE ABS(txn_amount_usd / (token0_inbalance + token0_outbalance))
                END AS price_usd
            FROM v2_swap_ethereum_corrected_1 
            WHERE token0_id IS NOT NULL
            AND utc_date >  (SELECT max_timestamp_unix FROM timestamp_daily_price_updation)
            AND utc_date <  EXTRACT(EPOCH FROM GETDATE())::bigint

            UNION ALL

            SELECT
                token1_id AS token,
                timestamp AS timestamp_column,
                CAST(logindex AS INT),
                token1_outbalance AS token_outbalance,
                token1_inbalance AS token_inbalance,
                txn_amount_usd,
                DATE_TRUNC('hour', DATEADD(second, CAST(timestamp AS INT), '1970-01-01')) + INTERVAL '30 minutes' AS utc_date,
                CASE
                    WHEN token1_inbalance + token1_outbalance = 0 THEN NULL
                    ELSE ABS(txn_amount_usd / (token1_inbalance + token1_outbalance))
                END AS price_usd
            FROM v2_swap_ethereum_corrected_1
            WHERE token1_id IS NOT NULL
            AND utc_date >  (SELECT max_timestamp_unix FROM timestamp_daily_price_updation)
            AND utc_date < EXTRACT(EPOCH FROM GETDATE())::bigint
        ) subquery1
    ) subquery2
    WHERE rn = 1;
    '''

    execute_query(q1)
    logging.debug('Query 1 execution completed')

    logging.debug('Executing query 2')
    q2 = '''
	UPDATE timestamp_daily_price_updation
	SET max_timestamp_unix = v2_swap_ethereum_corrected_1;'''
    execute_query(q2)
    logging.debug('Query 2 execution completed')


if __name__ == '__main__':
    update_daily_price()
