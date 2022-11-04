CREATE SCHEMA IF NOT EXISTS nxm;

CREATE TABLE IF NOT EXISTS nxm.quotation_trades (
    cid numeric,
    contract_address bytea,
    symbol text,
    evt_index integer,
    evt_tx_hash bytea,
    curr bytea,
    premium numeric,
    pre_amount numeric,
    premium_nxm numeric,
    pre_nxm_amount numeric,
    sc_add bytea,
    sum_assured numeric,
    block_hash bytea,
    nonce numeric,
    gas_limit numeric,
    gas_price numeric,
    gas_used numeric,
    max_fee_per_gas numeric,
    max_priority_fee_per_gas numeric,
    priority_fee_per_gas numeric,
    success bool,
    tx_type text,
    tx_value numeric,
    status_num numeric,
    cover_block_number integer,
    cover_block_time timestamptz,
    evt_block_number integer,
    evt_block_time timestamptz,
    evt_expiry numeric,
    evt_expiry_date timestamptz,
    PRIMARY KEY (evt_tx_hash, evt_index)
  );
  
--报价
CREATE OR REPLACE FUNCTION nxm.insert_quotation_trades(start_ts timestamptz, end_ts timestamptz = now()) RETURNS integer 
    LANGUAGE plpgsql AS $function$ 
    DECLARE r integer;
BEGIN
WITH
  rows AS (
    INSERT INTO
      nxm.quotation_trades (
        cid,
        contract_address,
        symbol,
        evt_index,
        evt_tx_hash,
        curr,
        premium,
        pre_amount,
        premium_nxm,
        pre_nxm_amount,
        sc_add,
        sum_assured,
        block_hash,
        nonce,
        gas_limit,
        gas_price,
        gas_used,
        max_fee_per_gas,
        max_priority_fee_per_gas,
        priority_fee_per_gas,
        success,
        tx_type,
        tx_value,
        status_num,
        cover_block_number,
        cover_block_time,
        evt_block_number,
        evt_block_time,
        evt_expiry,
        evt_expiry_date
      )
    select
      quo_evt.cid,
      quo_evt.contract_address,
      erc20.symbol,
      quo_evt.evt_index,
      quo_evt.evt_tx_hash,
      quo_evt."curr",
      quo_evt."premium",
      quo_evt."premium" / (10 ^ erc20.decimals) AS pre_amount,
      quo_evt."premiumNXM",
      quo_evt."premiumNXM" / (10 ^ erc20.decimals) AS preNXM_amount,
      quo_evt."scAdd",
      quo_evt."sumAssured",
      tx."block_hash",
      tx."nonce",
      tx."gas_limit",
      tx."gas_price",
      tx."gas_used",
      tx."max_fee_per_gas",
      tx."max_priority_fee_per_gas",
      tx."priority_fee_per_gas",
      tx."success",
      tx."type" as tx_type,
      tx."value" as tx_value,
      cse."statusNum",
      cse."evt_block_number" as cover_block_number,
      cse."evt_block_time" as cover_block_time,
      quo_evt."evt_block_number" as evt_block_number,
      quo_evt."evt_block_time" as evt_block_time,
      quo_evt."expiry" as evt_expiry,
      to_timestamp(quo_evt."expiry") as evt_expiry_date
    from
      (
        Select
          cid,
          contract_address,
          evt_block_number,
          evt_block_time,
          evt_index,
          evt_tx_hash,
          "curr",
          "expiry",
          "premium",
          "premiumNXM",
          "scAdd",
          "sumAssured",
          cast(
            '\xd7c49cee7e9188cca6ad8ff264c1da2e69d4cf3b' as bytea
          ) as token
        from
          nexusmutual."QuotationData_evt_CoverDetailsEvent"
        where 
          evt_block_time >= start_ts
          AND evt_block_time < end_ts
      ) quo_evt
      INNER JOIN ethereum.transactions tx ON quo_evt.evt_tx_hash = tx.hash
      and tx.block_time >= start_ts
      AND tx.block_time < end_ts
      INNER JOIN erc20.tokens erc20 on quo_evt.token = erc20.contract_address
      LEFT JOIN nexusmutual."QuotationData_evt_CoverStatusEvent" cse ON quo_evt.cid = cse.cid
      and cse.evt_block_time >= start_ts
      AND cse.evt_block_time < end_ts

      ON CONFLICT DO NOTHING
      RETURNING 1
  )
SELECT count(*) INTO r FROM rows;
RETURN r;
END $function$;

-- fill before 2022: This is only ever relevant 1 time.
SELECT
  nxm.insert_quotation_trades(
    '2019-05-30', --! Deployment date
    '2022-07-28')
WHERE
  NOT EXISTS(
    SELECT *
    FROM nxm.quotation_trades
    WHERE evt_block_time >= '2019-05-30' AND evt_block_time < '2022-07-27'
  );

INSERT INTO cron.job (schedule, command)
VALUES ('1 0 * * *',$$
    BEGIN;

DELETE FROM nxm.quotation_trades
WHERE evt_block_time >= (SELECT DATE_TRUNC('day', now()) - INTERVAL '3 months');

SELECT nxm.insert_quotation_trades(
    (SELECT DATE_TRUNC('day', now()) - INTERVAL '3 months')
  );
$$) 
ON CONFLICT (command) DO UPDATE SET schedule = EXCLUDED.schedule;
