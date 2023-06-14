UPDATE transfer_event_token_balances_at_different_intervals
        SET token_balance_t  = transfer_event_token_balances_at_different_intervals.token_balance_t + t2.total_token_balance
        FROM (
            SELECT address, token_id, date, total_token_balance
            FROM temp0
        ) AS t2
        WHERE transfer_event_token_balances_at_different_intervals.maker_addr = t2.address
            and transfer_event_token_balances_at_different_intervals.token_id = t2.token_id;