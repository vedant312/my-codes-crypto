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
   		AND utc_date <  (SELECT MAX(DATEADD(day, 1, utc_date)) FROM daily_token_transaction)
   		AND utc_date >  (SELECT MAX(utc_date) FROM daily_token_transaction)

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
   		AND utc_date <  (SELECT MAX(DATEADD(day, 1, utc_date)) FROM daily_token_transaction)
   		AND utc_date >  (SELECT MAX(utc_date) FROM daily_token_transaction)
    ) subquery1
) subquery2
WHERE rn = 1;
