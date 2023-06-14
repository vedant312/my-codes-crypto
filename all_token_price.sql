CREATE TABLE all_token_price AS
SELECT
    token,
    timestamp_column,
    price_usd
FROM (
    SELECT
        token0_id AS token,
        timestamp AS timestamp_column,
        token0_outbalance,
        token0_inbalance,
        txn_amount_usd,
         CASE
        WHEN token0_inbalance + token0_outbalance = 0 THEN NULL
        ELSE ABS(txn_amount_usd / (token0_inbalance + token0_outbalance))
    	END AS price_usd,
        ROW_NUMBER() OVER (PARTITION BY token0_id ORDER BY timestamp DESC) AS rn
    FROM v2_swap_ethereum_corrected_1
    WHERE token0_id IS NOT NULL

    UNION ALL

    SELECT
        token1_id AS token,
        timestamp AS timestamp_column,
        token1_outbalance,
        token1_inbalance,
        txn_amount_usd,
         CASE
        WHEN token1_inbalance + token1_outbalance = 0 THEN NULL
        ELSE ABS(txn_amount_usd / (token1_inbalance + token1_outbalance))
   		END AS price_usd,
        ROW_NUMBER() OVER (PARTITION BY token1_id ORDER BY timestamp DESC) AS rn
    FROM new_table_team2
) subquery
WHERE rn = 1
GROUP BY token, timestamp_column, price_usd;
