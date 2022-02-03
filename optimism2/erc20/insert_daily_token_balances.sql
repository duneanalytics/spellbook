CREATE OR REPLACE FUNCTION erc20.insert_daily_token_balances(start_block_time timestamptz, end_block_time timestamptz=now()) RETURNS integer
LANGUAGE plpgsql AS $function$
DECLARE r integer;
BEGIN
WITH rows AS (
    INSERT INTO erc20.daily_token_balances (
        day,
        user_address,
        token_address, 
        symbol,
        raw_value,
        token_value,
        median_price,
        usd_value
    )


SELECT
day, user_address, token_address, sumo.symbol, raw_value, token_value, 
AVG(dp.median_price) AS median_price,
token_value*AVG(dp.median_price) AS usd_value
FROM (
    SELECT 
    user_address, f.day, token_address, f.symbol,
	SUM(raw_value) OVER (PARTITION BY user_address, token_address ORDER BY day ASC) AS raw_value,
    	SUM(token_value) OVER (PARTITION BY user_address, token_address ORDER BY day ASC) AS token_value
    
    FROM (
    SELECT 
    user_address, day, token_address, symbol, SUM(value) raw_value, SUM(value/10^decimals) token_value
    
    FROM (
        
            SELECT user_address, '11-11-2021'::timestamp AS day, "contract_address" AS token_address, value FROM ovm1."erc20_balances"
            WHERE start_block_time <= '11-11-2021'::timestamp
            UNION ALL
	    
	    SELECT user_address, start_block_time AS day, token_address, raw_value
	    FROM (
		    SELECT user_address, day, token_address, raw_value,
		    DENSE_RANK() OVER(PARTITION BY user_address, token_address ORDER BY day DESC) AS user_token_rank
		    FROM erc20.daily_token_balances
		 ) at
	    WHERE user_token_rank = 1
            
            --ERC20s
            UNION ALL
            SELECT "from", DATE_TRUNC('day',evt_block_time) AS day, "contract_address" AS token_address, SUM(-value) AS value FROM erc20."ERC20_evt_Transfer" 
                WHERE evt_block_time BETWEEN start_block_time AND end_block_time GROUP BY 1,2, 3
            UNION ALL
            SELECT "to", DATE_TRUNC('day',evt_block_time) AS day, "contract_address" AS token_address, SUM(value) AS value FROM erc20."ERC20_evt_Transfer" 
                WHERE evt_block_time BETWEEN start_block_time AND end_block_time GROUP BY 1,2, 3
            
            --ETH Transfers
            
            UNION ALL
            SELECT "to", DATE_TRUNC('day',block_time) AS day, '\xdeaddeaddeaddeaddeaddeaddeaddeaddead0000'::bytea AS token_address, SUM(value) AS value
            FROM optimism."traces"
            WHERE (call_type NOT IN ('delegatecall', 'callcode', 'staticcall') or call_type is null)
            AND "tx_success" AND success
            AND block_time BETWEEN start_block_time AND end_block_time
            GROUP BY 1, 2, 3
            
            UNION ALL
            
            SELECT "from", DATE_TRUNC('day',block_time) AS day, '\xdeaddeaddeaddeaddeaddeaddeaddeaddead0000'::bytea AS token_address, SUM(-value) AS value
            FROM optimism."traces"
            WHERE (call_type NOT IN ('delegatecall', 'callcode', 'staticcall') or call_type is null)
            AND block_time BETWEEN start_block_time AND end_block_time
            AND "tx_success" AND success
            GROUP BY 1, 2, 3
            
            UNION ALL --gas costs (approximated)
            
            SELECT
            "from", DATE_TRUNC('day',t.block_time) AS day, '\xdeaddeaddeaddeaddeaddeaddeaddeaddead0000'::bytea AS token_address,
            -SUM(
                CASE WHEN gas_price = 0 THEN 0 ELSE
                (gas_used*gas_price)--l2 fees
                +
                get_fee_scalar(t.block_number)*(--l1 fees
                (get_l1_gas_used(t.data,t.block_number)) * (l1_gas_price*1e9) --if l2 gas price = 0, then all 0
                )
                END
            )
            AS value
            FROM optimism."transactions" t
            INNER JOIN ovm2."l1_gas_price_oracle_updates" gaso
                        ON t.block_number = gaso.block_number
                        AND t.block_time = gaso.block_time
            WHERE t.block_time BETWEEN start_block_time AND end_block_time
            AND gaso.block_time BETWEEN start_block_time AND end_block_time
            
            GROUP BY 1, 2,3
        ) bals
        LEFT JOIN erc20."tokens" e
            ON bals."token_address" = e."contract_address"
        
        GROUP BY 1,2,3, 4
    ) f
) sumo
LEFT JOIN prices."approx_prices_from_dex_data" dp
            ON dp."contract_address" = ( --If using the ETH placeholder address, pull the price for WETH address
                                        CASE WHEN sumo.token_address = '\xDeadDeAddeAddEAddeadDEaDDEAdDeaDDeAD0000'
                                            THEN '\x4200000000000000000000000000000000000006'
                                            ELSE sumo.token_address
                                        END
                                        )
            AND DATE_TRUNC('day',dp.hour) = sumo.day
WHERE user_address IS NOT NULL

GROUP BY day, user_address, token_address, sumo.symbol, raw_value, token_value
	
    -- update if we have new info on prices or the erc20
    ON CONFLICT (day, user_address, token_address)
    DO UPDATE SET
        symbol = EXCLUDED.symbol,
        raw_value = EXCLUDED.raw_value,
        token_value = EXCLUDED.token_value,
        median_price = EXCLUDED.median_price,
	usd_value = EXCLUDED.usd_value

    RETURNING 1
)
SELECT count(*) INTO r from rows;
RETURN r;
END
$function$;

-- fill Dec 2021 (post-regenesis 11-11)
SELECT erc20.insert_daily_token_balances(
    '2021-07-01'::timestamp,
    '2022-01-01'::timestamp
)
WHERE NOT EXISTS (
    SELECT *
    FROM erc20.daily_token_balances
    WHERE day >= '2021-07-01'
    AND day <= '2022-01-01'::timestamp
);

-- fill Jan 2022
SELECT erc20.insert_daily_token_balances(
    '2022-01-01'::timestamp,
    '2022-02-01'::timestamp
)
WHERE NOT EXISTS (
    SELECT *
    FROM erc20.daily_token_balances
    WHERE day > '2022-01-01'::timestamp
    AND day <= '2022-02-01'::timestamp
);

-- fill the rest 
SELECT erc20.insert_daily_token_balances(
    '2022-02-01'::timestamp,
    NOW()
)
WHERE NOT EXISTS (
    SELECT *
    FROM erc20.daily_token_balances
    WHERE day > '2022-02-01'::timestamp
    AND day <= NOW()
);

INSERT INTO cron.job (schedule, command)
VALUES ('15,45 * * * *', $$
    SELECT erc20.insert_daily_token_balances.sql(
        (SELECT max(block_time) - interval '3 days' FROM erc20.daily_token_balances),
        (SELECT now() - interval '5 minutes'),
        );
$$)
ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;
