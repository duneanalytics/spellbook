CREATE TABLE IF NOT EXISTS yearn.transactions (
    from_address bytea,
    to_address bytea,
    amount numeric,
    contract_address bytea,
    evt_tx_hash bytea NOT NULL,
    evt_index integer,
    evt_block_time timestamptz NOT NULL,
    evt_block_number numeric,
    yvault_deposit_token_symbol text,
    yvault_contract bytea,
    transaction_type text,
    yearn_type text,
    PRIMARY KEY (evt_tx_hash, evt_index)
);

CREATE OR REPLACE FUNCTION yearn.insert_yearn_transactions(start_ts timestamptz, end_ts timestamptz=now(), start_block numeric=0, end_block numeric=9e18) RETURNS integer
LANGUAGE plpgsql AS $function$
DECLARE r integer;
BEGIN
WITH rows AS (
    INSERT INTO yearn.transactions (
      from_address,
      to_address,
      amount,
      contract_address,
      evt_tx_hash,
      evt_index,
      evt_block_time,
      evt_block_number,
      yvault_deposit_token_symbol,
      yvault_contract,
      transaction_type,
      yearn_type
    )
    ((SELECT
    ett."from" AS from_address,
    ett."to" AS to_address,
    (ett."value" / (10^yct."yvault_deposit_token_decimals")) AS amount,
    ett."contract_address",
    ett."evt_tx_hash",
    ett."evt_index",
    ett."evt_block_time",
    ett."evt_block_number",
    yct."yvault_deposit_token_symbol",
    yct."yvault_contract",
    'deposit' as transaction_type,
    yct."yearn_type"
    FROM
    erc20."ERC20_evt_Transfer" ett 
    INNER JOIN yearn."view_yearn_contract_tokens" yct on (ett."contract_address" = yct."yvault_deposit_token" AND ett."to" = yct."yvault_contract")
    WHERE ett."evt_tx_hash" in (
        (SELECT "call_tx_hash" from yearn."yVault_call_deposit")
        UNION ALL
        (SELECT "call_tx_hash" from yearn."yVault_call_depositAll")
        UNION ALL
        (SELECT "call_tx_hash" from yearn_v2."yVault_call_deposit")
        UNION ALL
        (SELECT "call_tx_hash" from yearn_v2."yVault_call_deposit0")
        UNION ALL
        (SELECT "call_tx_hash" from yearn_v2."yVault_call_deposit1")
        UNION ALL
        (SELECT "call_tx_hash" from iearn_v2."yToken_call_deposit")
    )
    AND ett.evt_block_time >= start_ts
    AND ett.evt_block_time < end_ts
    AND ett.evt_block_number >= start_block
    AND ett.evt_block_number < end_block
    )

    UNION ALL

    --withdrawals from vaults
    (SELECT
    ett."from",
    ett."to",
    (ett."value" / (10^yct."yvault_deposit_token_decimals"))*-1 AS amount,
    ett."contract_address",
    ett."evt_tx_hash",
    ett."evt_index",
    ett."evt_block_time",
    ett."evt_block_number",
    yct."yvault_deposit_token_symbol",
    yct."yvault_contract",
    'withdrawal' AS transaction_type,
    yct."yearn_type"
    FROM
    erc20."ERC20_evt_Transfer" ett 
    INNER JOIN yearn."view_yearn_contract_tokens" yct on (ett."contract_address" = yct."yvault_deposit_token" AND ett."from" = yct."yvault_contract")
    WHERE ett."evt_tx_hash" in (
        (SELECT "call_tx_hash" from yearn."yVault_call_withdraw")
        UNION ALL
        (SELECT "call_tx_hash" from yearn."yVault_call_withdrawAll")
        UNION ALL
        (SELECT "call_tx_hash" from yearn_v2."yVault_call_withdraw")
        UNION ALL
        (SELECT "call_tx_hash" from yearn_v2."yVault_call_withdraw0")
        UNION ALL
        (SELECT "call_tx_hash" from yearn_v2."yVault_call_withdraw1")
        UNION ALL
        (SELECT "call_tx_hash" from yearn_v2."yVault_call_withdraw2")
        UNION ALL
        (SELECT "call_tx_hash" from iearn_v2."yToken_call_withdraw")
    )
    AND ett.evt_block_time >= start_ts
    AND ett.evt_block_time < end_ts
    AND ett.evt_block_number >= start_block
    AND ett.evt_block_number < end_block
    ))
    ON CONFLICT DO NOTHING
    RETURNING 1
)
SELECT count(*) INTO r from rows;
RETURN r;
END
$function$;

-- fill history
SELECT yearn.insert_yearn_transactions(
    '2020-01-01',
    '2021-01-01',
    (SELECT MAX(number) FROM ethereum.blocks WHERE time < '2020-01-01'),
    (SELECT MAX(number) FROM ethereum.blocks where time < '2021-01-01')
)
WHERE NOT EXISTS (
    SELECT *
    FROM yearn.transactions
    WHERE evt_block_time >= '2020-01-01'
    AND evt_block_time < '2021-01-01'
);

-- fill history
SELECT yearn.insert_yearn_transactions(
    '2021-01-01',
    '2022-01-01',
    (SELECT MAX(number) FROM ethereum.blocks WHERE time < '2021-01-01'),
    (SELECT MAX(number) FROM ethereum.blocks where time < '2022-01-01')
)
WHERE NOT EXISTS (
    SELECT *
    FROM yearn.transactions
    WHERE evt_block_time >= '2021-01-01'
    AND evt_block_time < '2022-01-01'
);

-- fill history
SELECT yearn.insert_yearn_transactions(
    '2022-01-01',
    now(),
    (SELECT MAX(number) FROM ethereum.blocks WHERE time < '2022-01-01'),
    (SELECT MAX(number) FROM ethereum.blocks where time < now() - interval '20 minutes')
)
WHERE NOT EXISTS (
    SELECT *
    FROM yearn.transactions
    WHERE evt_block_time >= '2022-01-01'
    AND evt_block_time < now() - interval '20 minutes'
);

INSERT INTO cron.job (schedule, command)
VALUES ('*/20 * * * *', $$
    SELECT yearn.insert_yearn_transactions(
        (SELECT MAX(evt_block_time) - interval '1 days' FROM yearn.transactions),
        (SELECT now() - interval '20 minutes'),
        (SELECT MAX(number) FROM ethereum.blocks WHERE time < (SELECT MAX(evt_block_time) - interval '1 days' FROM yearn.transactions)),
        (SELECT MAX(number) FROM ethereum.blocks where time < now() - interval '20 minutes'));
$$)
ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;
