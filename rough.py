def update_latest_token_balance_price_t():

    insert_timestamp = '''
        INSERT INTO next_timestamp_for_daily_token_transaction (timestamps)
        select max(timestamp) from v2_swap_ethereum_corrected_1;
    '''
    execute_query(insert_timestamp)

    # creating table to update daily token transactions and token balance table

    create_temp0_query = '''
        CREATE TABLE temp0 AS
        SELECT
          maker_addr,
          token0_id AS token_id,
          SUM(token0_inbalance) + SUM(token0_outbalance) AS token_transacted,
          DATE_TRUNC('day', TIMESTAMP '1970-01-01' + INTERVAL '1 second' * CAST(timestamp AS NUMERIC(20, 0))) AS utc_date
        FROM
          v2_swap_ethereum_corrected_1
        WHERE
          maker_addr IS NOT NULL
          AND timestamp > (SELECT max(timestamps) FROM last_timestamp_for_daily_token_transaction)
          AND timestamp < (SELECT max(timestamps) FROM next_timestamp_for_daily_token_transaction)
        GROUP BY
          maker_addr, token0_id, utc_date;
    '''

    execute_query(create_temp0_query)

    # Updating the tables using temp0 constructed

    update_query_temp0 = '''
        UPDATE daily_token_transaction
        SET token_transacted = daily_token_transaction.token_transacted + t0.token_transacted
        FROM temp0 t0
        WHERE
          daily_token_transaction.maker_addr = t0.maker_addr
          AND daily_token_transaction.token_id = t0.token_id
          AND daily_token_transaction.utc_date = t0.utc_date;
    '''

    insert_query_temp0 = '''
        INSERT INTO daily_token_transaction (maker_addr, token_id, token_transacted, utc_date)
        SELECT t0.maker_addr, t0.token_id, t0.token_transacted, t0.utc_date
        FROM temp0 t0
        WHERE NOT EXISTS (
          SELECT 1
          FROM daily_token_transaction t1
          WHERE t1.maker_addr = t0.maker_addr
            AND t1.token_id = t0.token_id
            AND t1.utc_date = t0.utc_date
        );
    '''
    execute_query(update_query_temp0)
    execute_query(insert_query_temp0)

    create_temp1_query = '''
        CREATE TABLE temp1 AS
        SELECT
          maker_addr,
          token1_id AS token_id,
          SUM(token1_inbalance) + SUM(token1_outbalance) AS token_transacted,
          DATE_TRUNC('day', TIMESTAMP '1970-01-01' + INTERVAL '1 second' * CAST(timestamp AS NUMERIC(20, 0))) AS utc_date
        FROM
          v2_swap_ethereum_corrected_1
        WHERE
          maker_addr IS NOT NULL
          AND timestamp > (SELECT max(timestamps) FROM last_timestamp_for_daily_token_transaction)
          AND timestamp < (SELECT max(timestamps) FROM next_timestamp_for_daily_token_transaction)
        GROUP BY
          maker_addr, token1_id, utc_date;
        '''
    execute_query(create_temp1_query)

    update_query_temp1 = '''
        UPDATE daily_token_transaction
        SET token_transacted = daily_token_transaction.token_transacted + t1.token_transacted
        FROM temp1 t1
        WHERE
          daily_token_transaction.maker_addr = t1.maker_addr
          AND daily_token_transaction.token_id = t1.token_id
          AND daily_token_transaction.utc_date = t1.utc_date;
    '''

    insert_query_temp1 = '''
        INSERT INTO daily_token_transaction (maker_addr, token_id, token_transacted, utc_date)
        SELECT t1.maker_addr, t1.token_id, t1.token_transacted, t1.utc_date
        FROM temp1 t1
        WHERE NOT EXISTS (
          SELECT 1
          FROM daily_token_transaction t0
          WHERE t0.maker_addr = t1.maker_addr
            AND t0.token_id = t1.token_id
            AND t0.utc_date = t1.utc_date
        );
    '''
    execute_query(update_query_temp1)
    execute_query(insert_query_temp1)

    update_t_balance_temp0 = '''
        UPDATE latest_token_balances 
        SET token_balance_t  = latest_token_balances.token_balance_t + t2.token_transacted
        FROM (
            SELECT maker_addr, token_id, token_transacted, utc_date
            FROM temp0
        ) AS t2
        WHERE latest_token_balances.token_id = t2.token_id
            and latest_token_balances.maker_addr = t2.maker_addr;
    '''

    insert_t_balance_temp0 = '''
        INSERT INTO latest_token_balances (maker_addr, token_id, token_balance_t)
        SELECT t2.maker_addr, t2.token_id, t2.token_transacted
        FROM (
            SELECT maker_addr, token_id, token_transacted, utc_date
            FROM temp0
        ) AS t2
        WHERE NOT EXISTS (
            SELECT 1
            FROM latest_token_balances
            WHERE token_id = t2.token_id
                AND maker_addr = t2.maker_addr
        );
    '''
    execute_query(update_t_balance_temp0)
    execute_query(insert_t_balance_temp0)

    update_t_balance_temp1 = '''
        UPDATE latest_token_balances 
        SET token_balance_t  = latest_token_balances.token_balance_t + t2.token_transacted
        FROM (
            SELECT maker_addr, token_id, token_transacted, utc_date
            FROM temp1
        ) AS t2
        WHERE latest_token_balances.token_id = t2.token_id
            and latest_token_balances.maker_addr = t2.maker_addr;
    '''

    insert_t_balance_temp1 = '''
        INSERT INTO latest_token_balances (maker_addr, token_id, token_balance_t)
        SELECT t2.maker_addr, t2.token_id, t2.token_transacted
        FROM (
            SELECT maker_addr, token_id, token_transacted, utc_date
            FROM temp1
        ) AS t2
        WHERE NOT EXISTS (
            SELECT 1
            FROM latest_token_balances
            WHERE token_id = t2.token_id
                AND maker_addr = t2.maker_addr
        );
    '''
    execute_query(update_t_balance_temp1)
    execute_query(insert_t_balance_temp1)

    drop_table_temp0 = '''
            drop table IF EXISTS temp0;
     '''

    drop_table_temp1 = '''
            drop table IF EXISTS temp1;
     '''
    execute_query(drop_table_temp0)
    execute_query(drop_table_temp1)
