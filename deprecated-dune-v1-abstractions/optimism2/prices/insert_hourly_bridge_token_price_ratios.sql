CREATE OR REPLACE FUNCTION prices.insert_hourly_bridge_token_price_ratios(start_time timestamptz, end_time timestamptz=now()) RETURNS integer
LANGUAGE plpgsql AS $function$
DECLARE r integer;
BEGIN

WITH bridge_tokens AS (
    SELECT --hop tokens
    DATE_TRUNC('hour',evt_block_time) AS dt,
        lp_contract, erc20_token, bridge_token, bridge_symbol, bridge_decimals,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY 
            ratio
            ) AS price_ratio,
            COUNT(*) AS sample_size
    FROM (
    SELECT
        s.evt_block_time,
        lp_contract, erc20_token, bridge_token, bridge_symbol, bridge_decimals,
        CASE WHEN "boughtId" = 0 THEN "tokensBought"::decimal/NULLIF("tokensSold",0)::decimal --if buy bridge_token then buys per sells is bridge_token price
                ELSE "tokensSold"::decimal/NULLIF("tokensBought",0)::decimal
                END
                AS ratio
        FROM hop_protocol."Swap_evt_TokenSwap" s
        INNER JOIN
            ( --map hToken to ERC20
            WITH e AS (
                SELECT al.contract_address AS lp_contract, e.contract_address AS token, t.symbol, t.decimals FROM hop_protocol."Swap_evt_AddLiquidity" al
                INNER JOIN erc20."ERC20_evt_Transfer" e
                    ON e.evt_tx_hash = al.evt_tx_hash
                INNER JOIN erc20."tokens" t
                    ON e.contract_address = t.contract_address
                WHERE (
                        LEFT(t.symbol,1) != 'h' --t is not h
                        )
                 AND LEFT(t.symbol,4) != 'HOP-'
		 AND al.evt_block_time >= start_time
                    AND al.evt_block_time < end_time
		 
                 GROUP BY 1,2,3,4
                )
            ,h AS (
            SELECT al.contract_address AS lp_contract, ht.contract_address AS token, ht.symbol, ht.decimals FROM hop_protocol."Swap_evt_AddLiquidity" al
                INNER JOIN erc20."ERC20_evt_Transfer" e
                    ON e.evt_tx_hash = al.evt_tx_hash
                INNER JOIN erc20."tokens" ht
                    ON e.contract_address = ht.contract_address
                WHERE (
                        LEFT(ht.symbol,1) = 'h' --ht is h
                        )
                 AND LEFT(ht.symbol,4) != 'HOP-'
		 AND al.evt_block_time >= start_time
                    AND al.evt_block_time < end_time
                 GROUP BY 1,2,3,4
                )
            SELECT e.lp_contract, e.token AS erc20_token, e.symbol AS erc20_symbol,
                                h.token AS bridge_token, h.symbol AS bridge_symbol, h.decimals AS bridge_decimals
            FROM e INNER JOIN h ON e.lp_contract = h.lp_contract
             ) m
    ON m.lp_contract = s."contract_address"
    GROUP BY 1,2,3,4,5,6,7
    
    UNION ALL
--Synapse Tokens
        SELECT
        block_time, NULL::bytea AS lp_contract, t.contract_address AS erc20_token, s.token AS bridge_token,
        et.symbol AS bridge_symbol, et.decimals AS bridge_decimals,
        t.value::decimal/NULLIF(s.amount,0)::decimal AS ratio
        FROM (
            SELECT
            "block_time", tx_hash,
            substring( l.topic2, 13, 20)::bytea AS sender, substring( l.topic2, 13, 20)::bytea AS receiver,
            substring( decode ( SUBSTRING ( encode(l."data", 'hex') , (64*1)+1 , 64 ), 'hex'),13,20)::bytea AS token,
            bytea2numeric ( decode ( SUBSTRING ( encode(l."data", 'hex') , (64*2)+1 , 64 ), 'hex')) AS amount
            
            FROM optimism.logs l
            
            WHERE l.contract_address = '\xaf41a65f786339e7911f4acdad6bd49426f2dc6b'
                AND l.topic1 IN ('\xdc5bad4651c5fbe9977a696aadc65996c468cde1448dd468ec0d83bf61c4b57c','\x91f25e9be0134ec851830e0e76dc71e06f9dade75a9b84e9524071dbbc319425')
	
		AND l.block_time >= start_time
                    AND l.block_time < end_time
            
	    ) s
        INNER JOIN erc20."ERC20_evt_Transfer" t
            ON t.evt_tx_hash = s.tx_hash
            AND t.contract_address != s.token
        LEFT JOIN erc20."tokens" et
            ON et."contract_address" = s.token
--            GROUP BY 1,2,3,4,5,6,7 --DISTINCT
	    
	WHERE t.evt_block_time >= start_time
            AND t.evt_block_time < end_time
    ) n
GROUP BY 1,2,3,4,5,6

),
final_prices AS (

SELECT dt, lp_contract,
CASE WHEN erc20_token IN ('\x121ab82b49b2bc4c7901ca46b8277962b4350204','\x1259adc9f2a0410d0db5e226563920a2d49f4454') --other WETHS
THEN '\x4200000000000000000000000000000000000006'
ELSE erc20_token END AS erc20_token

,bridge_token, bridge_symbol, bridge_decimals,price_ratio, sample_size

FROM bridge_tokens
),

rows AS (
    INSERT INTO prices.hourly_bridge_token_price_ratios (
    	hour,
	lp_contract,
	erc20_token,
	bridge_token,
	bridge_symbol,
	bridge_decimals,
	price_ratio,
	sample_size
    )

    SELECT 
        dt,
	lp_contract,
	erc20_token,
	bridge_token,
	bridge_symbol,
	bridge_decimals,
	price_ratio,
	sample_size
    FROM final_prices

    ON CONFLICT (bridge_token, hour) DO UPDATE SET price_ratio = EXCLUDED.price_ratio, sample_size = EXCLUDED.sample_size
    RETURNING 1
)
SELECT count(*) INTO r from rows;
RETURN r;
END
$function$;

-- Monthly backfill starting 11 Nov 2021 (regenesis
--TODO: Add pre-regenesis prices

SELECT prices.insert_hourly_bridge_token_price_ratios('2021-11-01', '2021-12-01');

SELECT prices.insert_hourly_bridge_token_price_ratios('2021-12-01', '2021-12-16');

-- Have the insert script run twice every hour at minute 15 and 45
-- `start-time` is set to go back three days in time so that entries can be retroactively updated 
-- in case `dex.trades` or price data falls behind.
INSERT INTO cron.job (schedule, command)
VALUES ('15,45 * * * *', $$
    SELECT prices.insert_hourly_bridge_token_price_ratios(
        (SELECT date_trunc('hour', now()) - interval '3 days'),
        (SELECT date_trunc('hour', now())));
$$)
ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;
