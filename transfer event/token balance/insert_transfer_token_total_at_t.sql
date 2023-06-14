INSERT INTO transfer_event_token_balances_at_different_intervals (maker_addr, token_id, token_balance_t)
        SELECT t2.account, t2.token_id, t2.total_token_balance
        FROM (
            SELECT account, token_id, date, total_token_balance
            FROM temp0
        ) AS t2
        WHERE NOT EXISTS (
            SELECT 1
            FROM transfer_event_token_balances_at_different_intervals
            WHERE token_id = t2.token_id
                AND maker_addr = t2.account