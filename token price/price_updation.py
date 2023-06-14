import logging
from execute_function import execute_query


def update_daily_price():
    logging.basicConfig(filename='logfile.log', level=logging.DEBUG,
                        format='%(asctime)s %(levelname)s: %(message)s')

    logging.debug('Executing query 1')
    q1 = '''UPDATE daily_all_token_price_in_usd
SET
    price_usd = subquery2.price_usd,
    timestamp_column = subquery2.timestamp_column,
    logindex = subquery2.logindex,
    transaction_date = subquery2.transaction_date
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
            DATE_TRUNC('day', DATEADD(second, CAST(timestamp AS INT), '1970-01-01')) AS utc_date,
            CASE
                WHEN token0_inbalance + token0_outbalance = 0 THEN NULL
                ELSE ABS(txn_amount_usd / (token0_inbalance + token0_outbalance))
            END AS price_usd
        FROM v2_swap_ethereum_corrected_1 
        WHERE token0_id IS NOT NULL
        AND timestamp_column  > (SELECT max_timestamp_unix FROM timestamp_daily_price_updation)

        UNION ALL

        SELECT
            token1_id AS token,
            timestamp AS timestamp_column,
            CAST(logindex AS INT),
            token1_outbalance AS token_outbalance,
            token1_inbalance AS token_inbalance,
            txn_amount_usd,
            DATE_TRUNC('day', DATEADD(second, CAST(timestamp AS INT), '1970-01-01')) AS utc_date,
            CASE
                WHEN token1_inbalance + token1_outbalance = 0 THEN NULL
                ELSE ABS(txn_amount_usd / (token1_inbalance + token1_outbalance))
            END AS price_usd
        FROM v2_swap_ethereum_corrected_1
        WHERE token1_id IS NOT NULL
        AND timestamp_column > (SELECT max_timestamp_unix FROM timestamp_daily_price_updation)
    ) subquery1
) subquery2
WHERE
    daily_all_token_price_in_usd.token = subquery2.token
    AND daily_all_token_price_in_usd.transaction_date = subquery2.transaction_date
    AND rn = 1;
    '''

    execute_query(q1)
    logging.debug('Query 1 execution completed')

    logging.debug('Executing query 2')
    q2 = '''INSERT INTO daily_all_token_price_in_usd (token, timestamp_column, logindex, price_usd, transaction_date)
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
        utc_date AS transaction_date,
        ROW_NUMBER() OVER (PARTITION BY token, transaction_date ORDER BY timestamp_column DESC, logindex DESC) AS rn
    FROM (
        SELECT
            token0_id AS token,
            timestamp AS timestamp_column,
            CAST(logindex AS INT),
            token0_outbalance AS token_outbalance,
            token0_inbalance AS token_inbalance,
            txn_amount_usd,
            DATE_TRUNC('day', DATEADD(second, CAST(timestamp AS INT), '1970-01-01')) AS utc_date,
            CASE
                WHEN token0_inbalance + token0_outbalance = 0 THEN NULL
                ELSE ABS(txn_amount_usd / (token0_inbalance + token0_outbalance))
            END AS price_usd
        FROM v2_swap_ethereum_corrected_1 
        WHERE token0_id IS NOT NULL
        AND timestamp_column > (SELECT max_timestamp_unix FROM timestamp_daily_price_updation)
        AND NOT EXISTS (
            SELECT 1
            FROM daily_all_token_price_in_usd
            WHERE daily_all_token_price_in_usd.token = v2_swap_ethereum_corrected_1.token0_id
            AND daily_all_token_price_in_usd.transaction_date = DATE_TRUNC('day', DATEADD(second, CAST(v2_swap_ethereum_corrected_1.timestamp AS INT), '1970-01-01'))
        )

        UNION ALL

        SELECT
            token1_id AS token,
            timestamp AS timestamp_column,
            CAST(logindex AS INT),
            token1_outbalance AS token_outbalance,
            token1_inbalance AS token_inbalance,
            txn_amount_usd,
            DATE_TRUNC('day', DATEADD(second, CAST(timestamp AS INT), '1970-01-01')) AS utc_date,
            CASE
                WHEN token1_inbalance + token1_outbalance = 0 THEN NULL
                ELSE ABS(txn_amount_usd / (token1_inbalance + token1_outbalance))
            END AS price_usd
        FROM v2_swap_ethereum_corrected_1
        WHERE token1_id IS NOT NULL
        AND timestamp_column > (SELECT max_timestamp_unix FROM timestamp_daily_price_updation)
        AND NOT EXISTS (
            SELECT 1
            FROM daily_all_token_price_in_usd
            WHERE daily_all_token_price_in_usd.token = v2_swap_ethereum_corrected_1.token1_id
            AND daily_all_token_price_in_usd.transaction_date = DATE_TRUNC('day', DATEADD(second, CAST(v2_swap_ethereum_corrected_1.timestamp AS INT), '1970-01-01'))
        )
    ) subquery1
) subquery2
WHERE rn = 1;
    '''

    execute_query(q2)
    logging.debug('Query 2 execution completed')

    logging.debug('Executing query 3')
    q3 = '''
	update timestamp_daily_price_updation
    set max_timestamp_unix  = (select max(timestamp) from v2_swap_ethereum_corrected_1);'''
    execute_query(q3)
    logging.debug('Query 3 execution completed')


if __name__ == '__main__':
    update_daily_price()
