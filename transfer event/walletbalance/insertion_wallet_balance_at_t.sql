INSERT INTO transfer_event_wallet_balance_in_usd (maker_addr, net_worth_t)
        SELECT maker_addr, net_worth_t
        FROM (
           SELECT maker_addr, 
                  SUM(CASE WHEN m.token_balance_t > 0 THEN m.token_balance_t * p.price_usd_t ELSE 0 END) AS net_worth_t
           FROM (
              SELECT maker_addr, token_id, token_balance_t
              FROM transfer_event_token_balances_at_different_intervals
           ) AS m
           JOIN (
              SELECT token_id, price_usd_t
              FROM price_history
           ) AS p ON m.token_id = p.token_id
           GROUP BY maker_addr
        ) AS subquery
        WHERE NOT EXISTS (
           SELECT 1
           FROM transfer_event_wallet_balance_in_usd n
           where n.maker_addr = subquery.maker_addr