create table transfer_event_wallet_balance_token_wise_per_day as
SELECT account, token_id,utc_date AS date, SUM(total_balance) AS total_token_balance
FROM
(
  SELECT te.from AS account, te.address AS token_id,DATE_TRUNC('day', DATEADD(second, CAST(timestamp AS INT), '1970-01-01')) AS utc_date,
    -CAST(te.value AS DOUBLE PRECISION) / POWER(10::DOUBLE PRECISION, td.token_decimals) AS total_balance
  FROM transfer_events AS te
  JOIN token_decimals AS td ON te.address = td.token_id
  WHERE td.chain_id = 1
  
  UNION ALL
  
  SELECT te.to AS account, te.address AS token_id,DATE_TRUNC('day', DATEADD(second, CAST(timestamp AS INT), '1970-01-01')) AS utc_date,
    CAST(te.value AS DOUBLE PRECISION) / POWER(10::DOUBLE PRECISION, td.token_decimals) AS total_balance
  FROM transfer_events AS te
  JOIN token_decimals AS td ON te.address = td.token_id
  WHERE td.chain_id = 1
) AS balances
GROUP BY account, token_id,date;