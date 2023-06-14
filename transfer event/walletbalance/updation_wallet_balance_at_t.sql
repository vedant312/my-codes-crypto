UPDATE transfer_event_wallet_balance_in_usd  AS tewb
        SET net_worth_t = subquery.net_worth_t
        FROM (
           SELECT maker_addr, 
                  SUM(CASE WHEN tetbdi.token_balance_t > 0 THEN tetbdi.token_balance_t * p.price_usd_t ELSE 0 END) AS net_worth_t
           FROM (
              SELECT maker_addr, token_id, token_balance_t
              FROM transfer_event_token_balances_at_different_intervals 
           ) AS tetbdi
           JOIN (
              SELECT token_id, price_usd_t
              FROM price_history
           ) AS p ON tetbdi.token_id = p.token_id
           GROUP BY maker_addr
        ) AS subquery
        WHERE tewb.maker_addr = subquery.maker_addr;