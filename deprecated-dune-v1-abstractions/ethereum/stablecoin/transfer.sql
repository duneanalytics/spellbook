CREATE TABLE stablecoin.transfer (
    "from" bytea,
    "to" bytea,
    coin_name text NOT NULL,
    symbol text,
    decimals numeric,
    contract_address bytea,
    amount numeric,
    amount_raw numeric,
    block_time timestamptz NOT NULL,
    tx_hash bytea,
    evt_index integer
);

CREATE OR REPLACE FUNCTION stablecoin.insert_transfer(start_ts timestamptz, end_ts timestamptz=now(), start_block numeric=0, end_block numeric=9e18) RETURNS integer
LANGUAGE plpgsql AS $function$
DECLARE r integer;
BEGIN
WITH transfers AS (
    SELECT  symbol,
            value / 10^decimals AS amount,
            "from",
            "to",
            name AS coin_name,
            tr.contract_address,
            value AS amount_raw,
            decimals,
            evt_block_time AS block_time,
            evt_tx_hash AS tx_hash,
            evt_index
    FROM erc20."ERC20_evt_Transfer" tr
    INNER JOIN erc20.stablecoins st ON tr.contract_address = st.contract_address
    WHERE tr.evt_block_time >= start_ts
            AND tr.evt_block_time < end_ts
),
rows AS (
    INSERT INTO stablecoin.transfer (
        symbol,
        amount,
        "from",
        "to",
        coin_name,
        contract_address,
        amount_raw,
        decimals,
        block_time,
        tx_hash,
        evt_index
    )
    SELECT
        symbol,
        amount,
        "from",
        "to",
        coin_name,
        contract_address,
        amount_raw,
        decimals,
        block_time,
        tx_hash,
        evt_index
    FROM transfers
    ON CONFLICT DO NOTHING
    RETURNING 1
)
SELECT count(*) INTO r from rows;
RETURN r;
END
$function$;

CREATE UNIQUE INDEX IF NOT EXISTS stablecoin_transfer_evt_index_uniq_idx ON stablecoin.transfer (tx_hash, evt_index);
CREATE INDEX IF NOT EXISTS stablecoin_transfer_block_time_idx ON stablecoin.transfer USING BRIN (block_time);

SELECT stablecoin.insert_transfer('2019-01-01', (SELECT now()), (SELECT max(number) FROM ethereum.blocks WHERE time < '2019-01-01'), (SELECT MAX(number) FROM ethereum.blocks)) WHERE NOT EXISTS (SELECT * FROM stablecoin.transfer LIMIT 1);
INSERT INTO cron.job (schedule, command)
VALUES ('14 1 * * *', $$SELECT stablecoin.insert_transfer((SELECT max(block_time) - interval '2 days' FROM stablecoin.transfer), (SELECT now()), (SELECT max(number) FROM ethereum.blocks WHERE time < (SELECT max(block_time) - interval '2 days' FROM stablecoin.transfer)), (SELECT MAX(number) FROM ethereum.blocks));$$)
ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;
