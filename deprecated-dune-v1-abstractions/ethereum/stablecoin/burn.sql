CREATE TABLE stablecoin.burn (
    burner bytea,
    amount numeric,
    symbol text,
    block_time timestamptz NOT NULL,
    "name" text NOT NULL,
    token_address bytea,
    to_address bytea,
    amount_raw numeric,
    decimals numeric,
    tx_hash bytea,
    evt_index integer,
    trace_address integer[]
);

CREATE OR REPLACE FUNCTION stablecoin.insert_burn(start_ts timestamptz, end_ts timestamptz=now(), start_block numeric=0, end_block numeric=9e18) RETURNS integer
LANGUAGE plpgsql AS $function$
DECLARE r integer;
BEGIN
WITH burn AS (
    SELECT "name",
           evt_block_time AS block_time,
           evt_tx_hash AS tx_hash,
           "to" AS to_address,
           "from" AS burner,
           st.contract_address AS token_address,
           st.symbol AS symbol,
           st.decimals,
           value / 10^st.decimals AS amount,
           value AS amount_raw,
           evt_index,
           trace_address
    FROM (
        SELECT evt_block_time,
               evt_tx_hash,
               "from", -- burner
               "to",
               contract_address,
               value,
               evt_index,
               NULL::integer[] as trace_address
        FROM erc20."ERC20_evt_Transfer" evt
        WHERE "to" = '\x0000000000000000000000000000000000000000'
       
        UNION ALL
  
        -- USDT
        SELECT evt_block_time,
               evt_tx_hash,
               "from", -- burner
               "to", -- USDT
               contract_address,
               value,
               evt_index,
               NULL::integer[]
        FROM erc20."ERC20_evt_Transfer" 
        WHERE contract_address = '\xdac17f958d2ee523a2206206994597c13d831ec7' 
        AND "to" = '\xc6cde7c39eb2f0f0095f41570af89efc2c1ea828'  -- USDT

        UNION ALL

        -- DSR Dai
        SELECT call_block_time,
               call_tx_hash,
               "src", -- from
               "dst",
               '\x6b175474e89094c44da98b954eedeac495271d0f', -- DAI
               "rad" / 1e27,
               NULL::integer,
               call_trace_address
        FROM makermcd."VAT_call_move" maker
        WHERE call_success = 'true'
        AND src = '\x197e90f9fad81970ba7976f33cbd77088e5d7cf7'
    ) burns
    INNER JOIN erc20.stablecoins st 
        ON st.contract_address = burns.contract_address
    WHERE burns.evt_block_time >= start_ts
        AND burns.evt_block_time < end_ts
),
rows AS (
    INSERT INTO stablecoin.burn (
       "name",
       block_time,
       tx_hash,
       burner,
       to_address,
       token_address,
       symbol,
       decimals,
       amount,
       amount_raw,
       evt_index,
       trace_address
    )
    SELECT
       "name",
       block_time,
       tx_hash,
       burner,
       to_address,
       token_address,
       symbol,
       decimals,
       amount,
       amount_raw,
       evt_index,
       trace_address
    FROM burn
    ON CONFLICT DO NOTHING
    RETURNING 1
)
SELECT count(*) INTO r from rows;
RETURN r;
END
$function$;

CREATE UNIQUE INDEX IF NOT EXISTS stablecoin_burn_tr_addr_uniq_idx ON stablecoin.burn (tx_hash, trace_address);
CREATE UNIQUE INDEX IF NOT EXISTS stablecoin_burn_evt_index_uniq_idx ON stablecoin.burn (tx_hash, evt_index);
CREATE INDEX IF NOT EXISTS stablecoin_burn_block_time_idx ON stablecoin.burn USING BRIN (block_time);

SELECT stablecoin.insert_burn('2019-01-01', (SELECT now()), (SELECT max(number) FROM ethereum.blocks WHERE time < '2019-01-01'), (SELECT MAX(number) FROM ethereum.blocks)) WHERE NOT EXISTS (SELECT * FROM stablecoin.burn LIMIT 1);
INSERT INTO cron.job (schedule, command)
VALUES ('14 1 * * *', $$SELECT stablecoin.insert_burn((SELECT max(block_time) - interval '2 days' FROM stablecoin.burn), (SELECT now()), (SELECT max(number) FROM ethereum.blocks WHERE time < (SELECT max(block_time) - interval '2 days' FROM stablecoin.burn)), (SELECT MAX(number) FROM ethereum.blocks));$$)
ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;
