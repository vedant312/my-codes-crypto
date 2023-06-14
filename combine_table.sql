--SELECT maker_addr, pair_id,is_weth,is_usdc,is_usdt,is_dai, SUM(net_token_change_0) AS wallet_balance_0, SUM(net_token_change_1) AS wallet_balance_1
--FROM (
--  SELECT maker_addr, pair_id,is_weth,is_usdc,is_usdt,is_dai,
--    (SUM(token0_inbalance) + SUM(token0_outbalance))  AS net_token_change_0 ,
--    (SUM(token1_inbalance) + SUM(token1_outbalance))  AS net_token_change_1
--  FROM new_table_team2 
--  GROUP BY maker_addr, pair_id,is_weth,is_usdc,is_usdt,is_dai, DATE_TRUNC('day', TIMESTAMP 'epoch' + CAST(timestamp AS BIGINT) * INTERVAL '1 second')
--) AS wb
----JOIN pairs AS p ON wb.pair_id = p.pair_id
--GROUP BY wb.maker_addr, wb.pair_id,is_weth,is_usdc,is_usdt,is_dai;
----GROUP BY maker_addr, pair_id,is_weth,is_usdc,is_usdt,is_dai;
SELECT
    wb.maker_addr,
    wb.pair_id,
    wb.base_token,wb.is_weth,wb.is_usdc,wb.is_usdt,wb.is_dai,
    SUM(wb.net_token_change_0) AS balance_token_0,
    SUM(wb.net_token_change_1) AS balance_token_1
    ,p.price_usd
  FROM (
    SELECT
      maker_addr,
      pair_id,
      base_token,is_weth,is_usdc,is_usdt,is_dai,
      SUM(token0_inbalance + token0_outbalance) AS net_token_change_0,
      SUM(token1_inbalance + token1_outbalance) AS net_token_change_1
    FROM new_table_team2
    GROUP BY maker_addr, pair_id, base_token,is_weth,is_usdc,is_usdt,is_dai, DATE_TRUNC('day', TIMESTAMP 'epoch' + CAST(timestamp AS BIGINT) * INTERVAL '1 second')
) AS wb
JOIN pairs AS p ON wb.pair_id = p.pair_id
--WHERE p.price_usd IS NOT NULL
GROUP BY wb.maker_addr,p.price_usd, wb.pair_id,wb.base_token,wb.is_weth,wb.is_usdc,wb.is_usdt,wb.is_dai;