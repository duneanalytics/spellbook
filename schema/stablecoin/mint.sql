CREATE TABLE stablecoin.mint (
    project text NOT NULL, 
    version text,
    block_time timestamptz NOT NULL,
    block_number numeric NOT NULL,
    tx_hash bytea,
    evt_index integer,
    trace_address integer[],
    minter bytea,
    tx_from bytea,
    asset_address bytea,
    asset_symbol text,
    token_amount numeric,
    usd_value numeric
);

CREATE OR REPLACE FUNCTION stablecoin.insert_mint(start_ts timestamptz, end_ts timestamptz=now(), start_block numeric=0, end_block numeric=9e18) RETURNS integer
LANGUAGE plpgsql AS $function$
DECLARE r integer;
BEGIN
WITH mint AS (
    SELECT "name" as project,
       "version",
       evt_block_time AS block_time,
       evt_block_number AS block_number,
       evt_tx_hash AS tx_hash,
       evt_index,
       trace_address,
       "to" AS minter,
       "from" AS tx_from,
       st.contract_address AS asset_address,
       st.symbol AS asset_symbol,
       value / 10^st.decimals AS token_amount,
       value / 10^st.decimals * p.price AS usd_value 
    FROM (
        -- all stablecoins that mint from \x0000....
        SELECT '1' as "version",
               evt_block_time,
               evt_block_number,
               evt_tx_hash,
               evt_index,
               NULL::integer[] as trace_address,
               "to", -- minter
               "from",
               contract_address,
               value
        FROM erc20."ERC20_evt_Transfer" evt
        WHERE "from" = '\x0000000000000000000000000000000000000000'
       
        UNION ALL
  
        -- USDT
        SELECT '1' as "version",
               evt_block_time,
               evt_block_number,
               evt_tx_hash,
               evt_index,
               NULL::integer[],
               "to", -- minter
               "from", -- USDT
               contract_address,
               value
        FROM erc20."ERC20_evt_Transfer" 
        WHERE contract_address = '\xdac17f958d2ee523a2206206994597c13d831ec7' 
        AND "from" = '\xc6cde7c39eb2f0f0095f41570af89efc2c1ea828'  -- USDT

        UNION ALL

        -- DSR Dai
        SELECT '2' as "version",
               call_block_time,
               call_block_number,
               call_tx_hash,
               NULL::integer,
               call_trace_address,
               "src",
               "dst", -- from
               '\x6b175474e89094c44da98b954eedeac495271d0f', -- DAI
               "rad" / 1e27
        FROM makermcd."VAT_call_move" maker
        WHERE call_success = 'true'
        AND dst = '\x197e90f9fad81970ba7976f33cbd77088e5d7cf7'
    ) mints
    INNER JOIN erc20.stablecoins st 
        ON st.contract_address = mints.contract_address
    LEFT JOIN prices.usd p 
        ON p.minute = date_trunc('minute', mints.evt_block_time) 
        AND p.contract_address = mints.contract_address 
        AND p.minute >= start_ts 
        AND p.minute < end_ts
),
rows AS (
    INSERT INTO stablecoin.mint (
       project,
       version,
       block_time,
       block_number,
       tx_hash,
       evt_index,
       trace_address,
       minter,
       tx_from,
       asset_address,
       asset_symbol,
       token_amount,
       usd_value
    )
    SELECT
       project,
       version,
       block_time,
       block_number,
       tx_hash,
       evt_index,
       trace_address,
       minter,
       tx_from,
       asset_address,
       asset_symbol,
       token_amount,
       usd_value
    FROM mint
    ON CONFLICT DO NOTHING
    RETURNING 1
)
SELECT count(*) INTO r from rows;
RETURN r;
END
$function$;

CREATE UNIQUE INDEX IF NOT EXISTS stablecoin_mint_tr_addr_uniq_idx ON stablecoin.mint (tx_hash, trace_address);
CREATE UNIQUE INDEX IF NOT EXISTS stablecoin_mint_evt_index_uniq_idx ON stablecoin.mint (tx_hash, evt_index);
CREATE INDEX IF NOT EXISTS stablecoin_mint_block_time_idx ON stablecoin.mint USING BRIN (block_time);

SELECT stablecoin.insert_mint('2019-01-01', (SELECT now()), (SELECT max(number) FROM ethereum.blocks WHERE time < '2019-01-01'), (SELECT MAX(number) FROM ethereum.blocks)) WHERE NOT EXISTS (SELECT * FROM stablecoin.mint LIMIT 1);
INSERT INTO cron.job (schedule, command)
VALUES ('14 1 * * *', $$SELECT stablecoin.insert_mint((SELECT max(block_time) - interval '2 days' FROM stablecoin.mint), (SELECT now()), (SELECT max(number) FROM ethereum.blocks WHERE time < (SELECT max(block_time) - interval '2 days' FROM stablecoin.mint)), (SELECT MAX(number) FROM ethereum.blocks));$$)
ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;
