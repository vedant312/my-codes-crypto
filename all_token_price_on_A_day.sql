SELECT
  token,
  timestamp_column,
  logindex,
  price_usd,
  transaction_date
FROM
  daily_all_token_price_in_usd 
WHERE
  transaction_date < '2023-01-06 00:00:00.000'
  AND (timestamp_column, logindex) IN (
    SELECT
      MAX(timestamp_column),
      MAX(logindex)
    FROM
      daily_all_token_price_in_usd 
    WHERE
      transaction_date < '2023-01-06 00:00:00.000'
    GROUP BY
      token
  );
