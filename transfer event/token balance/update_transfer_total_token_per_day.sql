update transfer_event_daily_token_balance
SET total_token_balance = transfer_event_daily_token_balance.total_token_balance + t0.total_token_balance
FROM temp0 as t0
WHERE
  transfer_event_daily_token_balance.account = t0.account
  AND transfer_event_daily_token_balance.token_id = t0.token_id
  AND transfer_event_daily_token_balance.date = t0.date;