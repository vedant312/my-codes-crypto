
SELECT
    token,
    timestamp_column,
    CAST(logindex AS INT),
    price_usd,
    transaction_date 
FROM (
    SELECT
        token,
        timestamp_column,
        CAST(logindex AS INT),
        price_usd,
        utc_date as transaction_date,
        ROW_NUMBER() OVER (PARTITION BY token, utc_date ORDER BY timestamp_column DESC, logindex DESC) AS rn
    FROM (
        SELECT
            token0_id AS token,
            timestamp AS timestamp_column,
            token0_outbalance as token_outbalance,
            token0_inbalance as token_inbalance,
            txn_amount_usd,
            CAST(logindex AS INT),
            DATE_TRUNC('day', DATEADD(second, CAST(timestamp AS INT), '1970-01-01')) AS utc_date,
            CASE
                WHEN token0_inbalance + token0_outbalance = 0 THEN NULL
                ELSE ABS(txn_amount_usd / (token0_inbalance + token0_outbalance))
            END AS price_usd
        FROM v2_swap_ethereum_corrected_1 
        WHERE token0_id IS NOT NULL

        UNION ALL

        SELECT
            token1_id AS token,
            timestamp AS timestamp_column,
            token1_outbalance as token_outbalance,
            token1_inbalance as token_inbalance,
            txn_amount_usd,
            CAST(logindex AS INT),
            DATE_TRUNC('day', DATEADD(second, CAST(timestamp AS INT), '1970-01-01')) AS utc_date,
            CASE
                WHEN token1_inbalance + token1_outbalance = 0 THEN NULL
                ELSE ABS(txn_amount_usd / (token1_inbalance + token1_outbalance))
            END AS price_usd
        FROM v2_swap_ethereum_corrected_1
        WHERE token1_id IS NOT NULL
    ) subquery1
) subquery2
WHERE rn = 1;
