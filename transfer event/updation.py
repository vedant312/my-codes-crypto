import logging
from base_function import execute_query


def update_latest_token_balance_price_t():
    logging.basicConfig(filename='logfile.log', level=logging.DEBUG,
                        format='%(asctime)s %(levelname)s: %(message)s')

    logging.debug('Executing query create temp0')
    create_temp0_query = '''
        create table temp0 as
        SELECT account, token_id,utc_date AS date, SUM(total_balance) AS total_token_balance
        FROM
        (
        SELECT te.from AS account, te.address AS token_id,DATE_TRUNC('day', DATEADD(second, CAST(timestamp AS INT), '1970-01-01')) AS utc_date,
            -CAST(te.value AS DOUBLE PRECISION) / POWER(10::DOUBLE PRECISION, td.token_decimals) AS total_balance
        FROM transfer_events AS te
        JOIN token_decimals AS td ON te.address = td.token_id
        WHERE td.chain_id = 1 and timestamp > (SELECT maxtimestamp FROM transfer_event_max_timestamp)
        
        UNION ALL
        
        SELECT te.to AS account, te.address AS token_id,DATE_TRUNC('day', DATEADD(second, CAST(timestamp AS INT), '1970-01-01')) AS utc_date,
            CAST(te.value AS DOUBLE PRECISION) / POWER(10::DOUBLE PRECISION, td.token_decimals) AS total_balance
        FROM transfer_events AS te
        JOIN token_decimals AS td ON te.address = td.token_id
        WHERE td.chain_id = 1 and timestamp > (SELECT maxtimestamp FROM transfer_event_max_timestamp)
        ) AS balances
        where account IS NOT NULL
        GROUP BY account, token_id,date;
    '''

    execute_query(create_temp0_query)
    logging.debug('Temp0 created successfully')

    # Updating the tables using temp0 constructed
    logging.debug('Executing query update daily token')
    update_query_daily_token = '''
        update transfer_event_daily_token_balance
        SET total_token_balance = transfer_event_daily_token_balance.total_token_balance + t0.total_token_balance
        FROM temp0 as t0
        WHERE
        transfer_event_daily_token_balance.account = t0.account
        AND transfer_event_daily_token_balance.token_id = t0.token_id
        AND transfer_event_daily_token_balance.date = t0.date;
    '''

    insert_query_daily_token = '''
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
    '''

    execute_query(insert_query_daily_token)
    execute_query(update_query_daily_token)

    logging.debug(
        'Updtaion of transfer_event_daily_token_balance by temp0 completed')

    logging.debug('Executing query updation of token balance at t')
    update_token_balance_t = '''
        UPDATE transfer_event_token_balances_at_different_intervals
        SET token_balance_t  = transfer_event_token_balances_at_different_intervals.token_balance_t + t2.total_token_balance
        FROM (
            SELECT account, token_id, date, total_token_balance
            FROM temp0
        ) AS t2
        WHERE transfer_event_token_balances_at_different_intervals.maker_addr = t2.account
            and transfer_event_token_balances_at_different_intervals.token_id = t2.token_id;
    '''

    insert_token_balance_t = '''
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
        );
    '''

    execute_query(insert_token_balance_t)
    execute_query(update_token_balance_t)

    logging.debug('Updation of token balances at t by temp0 completed')

    logging.debug('Executing query to drop temp0')
    drop_table_temp0 = '''
            drop table IF EXISTS temp0;
     '''

    execute_query(drop_table_temp0)
    logging.debug('Temp0 dropped')

    logging.debug('Executing query for timestamp')
    insert_timestamp = '''
        update transfer_event_max_timestamp
        set maxtimestamp  = (select max(timestamp) from transfer_events);
    '''
    execute_query(insert_timestamp)
    logging.debug('Timestamp inserted into last_timestamp table')


def update_net_worth():
    net_worth_t_update_query = '''
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
    '''

    net_worth_t_insert_query = '''
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
        );
    '''

    execute_query(net_worth_t_insert_query)
    execute_query(net_worth_t_update_query)

    logging.debug('Net worth t updated')


if __name__ == '__main__':
    update_latest_token_balance_price_t()
    update_net_worth()
