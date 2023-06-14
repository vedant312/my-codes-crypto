SELECT
    wb.maker_addr,
    SUM(COALESCE(balance_token_0_updated, 0) + COALESCE(balance_token_1_updated, 0)) AS total_balance
FROM (
    SELECT
        wb_inner.maker_addr,
        wb_inner.pair_id,
        wb_inner.base_token,
        wb_inner.is_weth,
        wb_inner.is_usdc,
        wb_inner.is_usdt,
        wb_inner.is_dai,
        CASE
            WHEN wb_inner.base_token = 1 AND wb_inner.balance_token_0 > 0 THEN wb_inner.balance_token_0 * p.price_usd
            WHEN wb_inner.base_token = 0 AND wb_inner.balance_token_1 > 0 THEN wb_inner.balance_token_1 * p.price_usd
            ELSE 0
        END AS balance_token_0_updated,
        CASE
            WHEN wb_inner.base_token = 1 AND wb_inner.balance_token_1 > 0 THEN
                CASE WHEN wb_inner.is_weth = 1 THEN wb_inner.balance_token_1 * weth_price_usd ELSE wb_inner.balance_token_1 END
            WHEN wb_inner.base_token = 0 AND wb_inner.balance_token_0 > 0 THEN
                CASE WHEN wb_inner.is_weth = 1 THEN wb_inner.balance_token_0 * weth_price_usd ELSE wb_inner.balance_token_0 END
            ELSE 0
        END AS balance_token_1_updated,
        p.price_usd,
        weth_price_usd
    FROM (
        SELECT
            maker_addr,
            pair_id,
            base_token,
            is_weth,
            is_usdc,
            is_usdt,
            is_dai,
            SUM(token0_inbalance + token0_outbalance) AS balance_token_0,
            SUM(token1_inbalance + token1_outbalance) AS balance_token_1
        FROM new_table_team2
        GROUP BY maker_addr, pair_id, base_token, is_weth, is_usdc, is_usdt, is_dai, DATE_TRUNC('day', TIMESTAMP 'epoch' + CAST(timestamp AS BIGINT) * INTERVAL '1 second')
    ) AS wb_inner
    JOIN pairs AS p ON wb_inner.pair_id = p.pair_id
    JOIN (
        SELECT weth_price_usd 
        FROM weth_history
        ORDER BY timestamp  DESC
        LIMIT 1
    ) AS wh ON 1=1  -- Cross join with a subquery to get the eth_price
) AS wb
GROUP BY wb.maker_addr
HAVING SUM(COALESCE(balance_token_0_updated, 0) + COALESCE(balance_token_1_updated, 0)) > 0;
