UPDATE transfer_event_token_balances_at_different_intervals AS tetbdi
SET token_balance_t_minus_182 = tetbdi.token_balance_t - COALESCE((
  SELECT SUM(tedtb.total_token_balance)
  FROM transfer_event_daily_token_balance AS tedtb
  WHERE tedtb.account = tetbdi.maker_addr
    AND tedtb.token_id = tetbdi.token_id
    AND tedtb.date > (
      SELECT MAX(DATEADD(day, -182, date))
      FROM transfer_event_daily_token_balance
    )
), 0);