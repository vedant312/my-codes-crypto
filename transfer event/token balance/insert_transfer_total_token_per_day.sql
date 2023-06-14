INSERT INTO transfer_event_daily_token_balance (account, token_id, date, total_token_balance)
        SELECT t0.account, t0.token_id, t0.date, t0.total_token_balance
        FROM temp0 as t0
        WHERE NOT EXISTS (
          SELECT 1
          FROM transfer_event_daily_token_balance as t1
          WHERE t1.account = t0.account
            AND t1.token_id = t0.token_id
            AND t1.date = t0.date
        );