SELECT
    CASE WHEN t.token0_outbalance = 0 THEN t.txn_amount_usd / t.token0_inbalance
         ELSE t.txn_amount_usd / t.token0_outbalance
    END AS price_usd_token_0,
    CASE WHEN t.token1_outbalance = 0 THEN t.txn_amount_usd / t.token1_inbalance
         ELSE t.txn_amount_usd / t.token1_outbalance
    END AS price_usd_token_1,
    t.timestamp,
    t.pair_id,t.logindex ,t.token0_outbalance ,t.token0_inbalance ,t.token1_outbalance ,t.token1_inbalance ,t.txn_amount_usd
FROM
    (SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY pair_id ORDER BY timestamp DESC, logindex DESC) AS row_num
    FROM
        new_table_team2 
    ) AS t
WHERE
    t.row_num = 1;
