CREATE OR REPLACE FUNCTION erc20.insert_weth_balance_changes(start_ts timestamptz, end_ts timestamptz=now()) RETURNS integer
LANGUAGE plpgsql AS $function$
DECLARE r integer;
BEGIN
WITH all_transfers AS (
    SELECT
        dst AS wallet_address,
        date_trunc('hour', evt_block_time) AS hour,
        wad AS amount
    FROM zeroex."WETH9_evt_Transfer" t
    WHERE evt_block_time >= start_ts and evt_block_time < end_ts
    UNION ALL
    SELECT
        src,
        date_trunc('hour', evt_block_time),
        - wad
    FROM zeroex."WETH9_evt_Transfer" t
    WHERE evt_block_time >= start_ts and evt_block_time < end_ts
    UNION ALL
    SELECT
        dst,
        date_trunc('hour', evt_block_time),
        wad
    FROM zeroex."WETH9_evt_Deposit" t
    WHERE evt_block_time >= start_ts and evt_block_time < end_ts
    UNION ALL
    SELECT
        src,
        date_trunc('hour', evt_block_time),
        - wad
    FROM zeroex."WETH9_evt_Withdrawal" t
    WHERE evt_block_time >= start_ts and evt_block_time < end_ts
),
rows AS (
    INSERT INTO erc20.weth_hourly_balance_changes (
        hour,
        wallet_address,
        token_address,
        amount_raw
    )

    SELECT
        hour,
        wallet_address,
        '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'::BYTEA AS token_address,
        SUM(amount) AS amount_raw
    FROM all_transfers t
    GROUP BY 1, 2, 3

    ON CONFLICT (hour, wallet_address, token_address) DO UPDATE SET amount_raw=EXCLUDED.amount_raw
    RETURNING 1
)
SELECT count(*) INTO r from rows;
RETURN r;
END
$function$;

-- `WETH` contract deployed on 2017-12-12
-- Weekly backfill
SELECT erc20.insert_weth_balance_changes('2017-12-12','2017-12-18') WHERE NOT EXISTS (SELECT * FROM erc20.weth_hourly_balance_changes WHERE hour >= '2017-12-12' AND hour < '2017-12-18');
SELECT erc20.insert_weth_balance_changes('2017-12-18','2017-12-25') WHERE NOT EXISTS (SELECT * FROM erc20.weth_hourly_balance_changes WHERE hour >= '2017-12-18' AND hour < '2017-12-25');
SELECT erc20.insert_weth_balance_changes('2017-12-25','2018-01-01') WHERE NOT EXISTS (SELECT * FROM erc20.weth_hourly_balance_changes WHERE hour >= '2017-12-25' AND hour < '2018-01-01');
SELECT erc20.insert_weth_balance_changes('2018-01-01','2018-01-08') WHERE NOT EXISTS (SELECT * FROM erc20.weth_hourly_balance_changes WHERE hour >= '2018-01-01' AND hour < '2018-01-08');
SELECT erc20.insert_weth_balance_changes('2018-01-08','2018-01-15') WHERE NOT EXISTS (SELECT * FROM erc20.weth_hourly_balance_changes WHERE hour >= '2018-01-08' AND hour < '2018-01-15');
SELECT erc20.insert_weth_balance_changes('2018-01-15','2018-01-22') WHERE NOT EXISTS (SELECT * FROM erc20.weth_hourly_balance_changes WHERE hour >= '2018-01-15' AND hour < '2018-01-22');
SELECT erc20.insert_weth_balance_changes('2018-01-22','2018-01-29') WHERE NOT EXISTS (SELECT * FROM erc20.weth_hourly_balance_changes WHERE hour >= '2018-01-22' AND hour < '2018-01-29');
SELECT erc20.insert_weth_balance_changes('2018-01-29','2018-02-05') WHERE NOT EXISTS (SELECT * FROM erc20.weth_hourly_balance_changes WHERE hour >= '2018-01-29' AND hour < '2018-02-05');
SELECT erc20.insert_weth_balance_changes('2018-02-05','2018-02-12') WHERE NOT EXISTS (SELECT * FROM erc20.weth_hourly_balance_changes WHERE hour >= '2018-02-05' AND hour < '2018-02-12');
SELECT erc20.insert_weth_balance_changes('2018-02-12','2018-02-19') WHERE NOT EXISTS (SELECT * FROM erc20.weth_hourly_balance_changes WHERE hour >= '2018-02-12' AND hour < '2018-02-19');
SELECT erc20.insert_weth_balance_changes('2018-02-19','2018-02-26') WHERE NOT EXISTS (SELECT * FROM erc20.weth_hourly_balance_changes WHERE hour >= '2018-02-19' AND hour < '2018-02-26');
SELECT erc20.insert_weth_balance_changes('2018-02-26','2018-03-05') WHERE NOT EXISTS (SELECT * FROM erc20.weth_hourly_balance_changes WHERE hour >= '2018-02-26' AND hour < '2018-03-05');
SELECT erc20.insert_weth_balance_changes('2018-03-05','2018-03-12') WHERE NOT EXISTS (SELECT * FROM erc20.weth_hourly_balance_changes WHERE hour >= '2018-03-05' AND hour < '2018-03-12');
SELECT erc20.insert_weth_balance_changes('2018-03-12','2018-03-19') WHERE NOT EXISTS (SELECT * FROM erc20.weth_hourly_balance_changes WHERE hour >= '2018-03-12' AND hour < '2018-03-19');
SELECT erc20.insert_weth_balance_changes('2018-03-19','2018-03-26') WHERE NOT EXISTS (SELECT * FROM erc20.weth_hourly_balance_changes WHERE hour >= '2018-03-19' AND hour < '2018-03-26');
SELECT erc20.insert_weth_balance_changes('2018-03-26','2018-04-02') WHERE NOT EXISTS (SELECT * FROM erc20.weth_hourly_balance_changes WHERE hour >= '2018-03-26' AND hour < '2018-04-02');
SELECT erc20.insert_weth_balance_changes('2018-04-02','2018-04-09') WHERE NOT EXISTS (SELECT * FROM erc20.weth_hourly_balance_changes WHERE hour >= '2018-04-02' AND hour < '2018-04-09');
SELECT erc20.insert_weth_balance_changes('2018-04-09','2018-04-16') WHERE NOT EXISTS (SELECT * FROM erc20.weth_hourly_balance_changes WHERE hour >= '2018-04-09' AND hour < '2018-04-16');
SELECT erc20.insert_weth_balance_changes('2018-04-16','2018-04-23') WHERE NOT EXISTS (SELECT * FROM erc20.weth_hourly_balance_changes WHERE hour >= '2018-04-16' AND hour < '2018-04-23');
SELECT erc20.insert_weth_balance_changes('2018-04-23','2018-04-30') WHERE NOT EXISTS (SELECT * FROM erc20.weth_hourly_balance_changes WHERE hour >= '2018-04-23' AND hour < '2018-04-30');
SELECT erc20.insert_weth_balance_changes('2018-04-30','2018-05-07') WHERE NOT EXISTS (SELECT * FROM erc20.weth_hourly_balance_changes WHERE hour >= '2018-04-30' AND hour < '2018-05-07');
SELECT erc20.insert_weth_balance_changes('2018-05-07','2018-05-14') WHERE NOT EXISTS (SELECT * FROM erc20.weth_hourly_balance_changes WHERE hour >= '2018-05-07' AND hour < '2018-05-14');
SELECT erc20.insert_weth_balance_changes('2018-05-14','2018-05-21') WHERE NOT EXISTS (SELECT * FROM erc20.weth_hourly_balance_changes WHERE hour >= '2018-05-14' AND hour < '2018-05-21');
SELECT erc20.insert_weth_balance_changes('2018-05-21','2018-05-28') WHERE NOT EXISTS (SELECT * FROM erc20.weth_hourly_balance_changes WHERE hour >= '2018-05-21' AND hour < '2018-05-28');
SELECT erc20.insert_weth_balance_changes('2018-05-28','2018-06-04') WHERE NOT EXISTS (SELECT * FROM erc20.weth_hourly_balance_changes WHERE hour >= '2018-05-28' AND hour < '2018-06-04');
SELECT erc20.insert_weth_balance_changes('2018-06-04','2018-06-11') WHERE NOT EXISTS (SELECT * FROM erc20.weth_hourly_balance_changes WHERE hour >= '2018-06-04' AND hour < '2018-06-11');
SELECT erc20.insert_weth_balance_changes('2018-06-11','2018-06-18') WHERE NOT EXISTS (SELECT * FROM erc20.weth_hourly_balance_changes WHERE hour >= '2018-06-11' AND hour < '2018-06-18');
SELECT erc20.insert_weth_balance_changes('2018-06-18','2018-06-25') WHERE NOT EXISTS (SELECT * FROM erc20.weth_hourly_balance_changes WHERE hour >= '2018-06-18' AND hour < '2018-06-25');
SELECT erc20.insert_weth_balance_changes('2018-06-25','2018-07-02') WHERE NOT EXISTS (SELECT * FROM erc20.weth_hourly_balance_changes WHERE hour >= '2018-06-25' AND hour < '2018-07-02');
SELECT erc20.insert_weth_balance_changes('2018-07-02','2018-07-09') WHERE NOT EXISTS (SELECT * FROM erc20.weth_hourly_balance_changes WHERE hour >= '2018-07-02' AND hour < '2018-07-09');
SELECT erc20.insert_weth_balance_changes('2018-07-09','2018-07-16') WHERE NOT EXISTS (SELECT * FROM erc20.weth_hourly_balance_changes WHERE hour >= '2018-07-09' AND hour < '2018-07-16');
SELECT erc20.insert_weth_balance_changes('2018-07-16','2018-07-23') WHERE NOT EXISTS (SELECT * FROM erc20.weth_hourly_balance_changes WHERE hour >= '2018-07-16' AND hour < '2018-07-23');
SELECT erc20.insert_weth_balance_changes('2018-07-23','2018-07-30') WHERE NOT EXISTS (SELECT * FROM erc20.weth_hourly_balance_changes WHERE hour >= '2018-07-23' AND hour < '2018-07-30');
SELECT erc20.insert_weth_balance_changes('2018-07-30','2018-08-06') WHERE NOT EXISTS (SELECT * FROM erc20.weth_hourly_balance_changes WHERE hour >= '2018-07-30' AND hour < '2018-08-06');
SELECT erc20.insert_weth_balance_changes('2018-08-06','2018-08-13') WHERE NOT EXISTS (SELECT * FROM erc20.weth_hourly_balance_changes WHERE hour >= '2018-08-06' AND hour < '2018-08-13');
SELECT erc20.insert_weth_balance_changes('2018-08-13','2018-08-20') WHERE NOT EXISTS (SELECT * FROM erc20.weth_hourly_balance_changes WHERE hour >= '2018-08-13' AND hour < '2018-08-20');
SELECT erc20.insert_weth_balance_changes('2018-08-20','2018-08-27') WHERE NOT EXISTS (SELECT * FROM erc20.weth_hourly_balance_changes WHERE hour >= '2018-08-20' AND hour < '2018-08-27');
SELECT erc20.insert_weth_balance_changes('2018-08-27','2018-09-03') WHERE NOT EXISTS (SELECT * FROM erc20.weth_hourly_balance_changes WHERE hour >= '2018-08-27' AND hour < '2018-09-03');
SELECT erc20.insert_weth_balance_changes('2018-09-03','2018-09-10') WHERE NOT EXISTS (SELECT * FROM erc20.weth_hourly_balance_changes WHERE hour >= '2018-09-03' AND hour < '2018-09-10');
SELECT erc20.insert_weth_balance_changes('2018-09-10','2018-09-17') WHERE NOT EXISTS (SELECT * FROM erc20.weth_hourly_balance_changes WHERE hour >= '2018-09-10' AND hour < '2018-09-17');
SELECT erc20.insert_weth_balance_changes('2018-09-17','2018-09-24') WHERE NOT EXISTS (SELECT * FROM erc20.weth_hourly_balance_changes WHERE hour >= '2018-09-17' AND hour < '2018-09-24');
SELECT erc20.insert_weth_balance_changes('2018-09-24','2018-10-01') WHERE NOT EXISTS (SELECT * FROM erc20.weth_hourly_balance_changes WHERE hour >= '2018-09-24' AND hour < '2018-10-01');
SELECT erc20.insert_weth_balance_changes('2018-10-01','2018-10-08') WHERE NOT EXISTS (SELECT * FROM erc20.weth_hourly_balance_changes WHERE hour >= '2018-10-01' AND hour < '2018-10-08');
SELECT erc20.insert_weth_balance_changes('2018-10-08','2018-10-15') WHERE NOT EXISTS (SELECT * FROM erc20.weth_hourly_balance_changes WHERE hour >= '2018-10-08' AND hour < '2018-10-15');
SELECT erc20.insert_weth_balance_changes('2018-10-15','2018-10-22') WHERE NOT EXISTS (SELECT * FROM erc20.weth_hourly_balance_changes WHERE hour >= '2018-10-15' AND hour < '2018-10-22');
SELECT erc20.insert_weth_balance_changes('2018-10-22','2018-10-29') WHERE NOT EXISTS (SELECT * FROM erc20.weth_hourly_balance_changes WHERE hour >= '2018-10-22' AND hour < '2018-10-29');
SELECT erc20.insert_weth_balance_changes('2018-10-29','2018-11-05') WHERE NOT EXISTS (SELECT * FROM erc20.weth_hourly_balance_changes WHERE hour >= '2018-10-29' AND hour < '2018-11-05');
SELECT erc20.insert_weth_balance_changes('2018-11-05','2018-11-12') WHERE NOT EXISTS (SELECT * FROM erc20.weth_hourly_balance_changes WHERE hour >= '2018-11-05' AND hour < '2018-11-12');
SELECT erc20.insert_weth_balance_changes('2018-11-12','2018-11-19') WHERE NOT EXISTS (SELECT * FROM erc20.weth_hourly_balance_changes WHERE hour >= '2018-11-12' AND hour < '2018-11-19');
SELECT erc20.insert_weth_balance_changes('2018-11-19','2018-11-26') WHERE NOT EXISTS (SELECT * FROM erc20.weth_hourly_balance_changes WHERE hour >= '2018-11-19' AND hour < '2018-11-26');
SELECT erc20.insert_weth_balance_changes('2018-11-26','2018-12-03') WHERE NOT EXISTS (SELECT * FROM erc20.weth_hourly_balance_changes WHERE hour >= '2018-11-26' AND hour < '2018-12-03');
SELECT erc20.insert_weth_balance_changes('2018-12-03','2018-12-10') WHERE NOT EXISTS (SELECT * FROM erc20.weth_hourly_balance_changes WHERE hour >= '2018-12-03' AND hour < '2018-12-10');
SELECT erc20.insert_weth_balance_changes('2018-12-10','2018-12-17') WHERE NOT EXISTS (SELECT * FROM erc20.weth_hourly_balance_changes WHERE hour >= '2018-12-10' AND hour < '2018-12-17');
SELECT erc20.insert_weth_balance_changes('2018-12-17','2018-12-24') WHERE NOT EXISTS (SELECT * FROM erc20.weth_hourly_balance_changes WHERE hour >= '2018-12-17' AND hour < '2018-12-24');
SELECT erc20.insert_weth_balance_changes('2018-12-24','2018-12-31') WHERE NOT EXISTS (SELECT * FROM erc20.weth_hourly_balance_changes WHERE hour >= '2018-12-24' AND hour < '2018-12-31');
SELECT erc20.insert_weth_balance_changes('2018-12-31','2019-01-07') WHERE NOT EXISTS (SELECT * FROM erc20.weth_hourly_balance_changes WHERE hour >= '2018-12-31' AND hour < '2019-01-07');
SELECT erc20.insert_weth_balance_changes('2019-01-07','2019-01-14') WHERE NOT EXISTS (SELECT * FROM erc20.weth_hourly_balance_changes WHERE hour >= '2019-01-07' AND hour < '2019-01-14');
SELECT erc20.insert_weth_balance_changes('2019-01-14','2019-01-21') WHERE NOT EXISTS (SELECT * FROM erc20.weth_hourly_balance_changes WHERE hour >= '2019-01-14' AND hour < '2019-01-21');
SELECT erc20.insert_weth_balance_changes('2019-01-21','2019-01-28') WHERE NOT EXISTS (SELECT * FROM erc20.weth_hourly_balance_changes WHERE hour >= '2019-01-21' AND hour < '2019-01-28');
SELECT erc20.insert_weth_balance_changes('2019-01-28','2019-02-04') WHERE NOT EXISTS (SELECT * FROM erc20.weth_hourly_balance_changes WHERE hour >= '2019-01-28' AND hour < '2019-02-04');
SELECT erc20.insert_weth_balance_changes('2019-02-04','2019-02-11') WHERE NOT EXISTS (SELECT * FROM erc20.weth_hourly_balance_changes WHERE hour >= '2019-02-04' AND hour < '2019-02-11');
SELECT erc20.insert_weth_balance_changes('2019-02-11','2019-02-18') WHERE NOT EXISTS (SELECT * FROM erc20.weth_hourly_balance_changes WHERE hour >= '2019-02-11' AND hour < '2019-02-18');
SELECT erc20.insert_weth_balance_changes('2019-02-18','2019-02-25') WHERE NOT EXISTS (SELECT * FROM erc20.weth_hourly_balance_changes WHERE hour >= '2019-02-18' AND hour < '2019-02-25');
SELECT erc20.insert_weth_balance_changes('2019-02-25','2019-03-04') WHERE NOT EXISTS (SELECT * FROM erc20.weth_hourly_balance_changes WHERE hour >= '2019-02-25' AND hour < '2019-03-04');
SELECT erc20.insert_weth_balance_changes('2019-03-04','2019-03-11') WHERE NOT EXISTS (SELECT * FROM erc20.weth_hourly_balance_changes WHERE hour >= '2019-03-04' AND hour < '2019-03-11');
SELECT erc20.insert_weth_balance_changes('2019-03-11','2019-03-18') WHERE NOT EXISTS (SELECT * FROM erc20.weth_hourly_balance_changes WHERE hour >= '2019-03-11' AND hour < '2019-03-18');
SELECT erc20.insert_weth_balance_changes('2019-03-18','2019-03-25') WHERE NOT EXISTS (SELECT * FROM erc20.weth_hourly_balance_changes WHERE hour >= '2019-03-18' AND hour < '2019-03-25');
SELECT erc20.insert_weth_balance_changes('2019-03-25','2019-04-01') WHERE NOT EXISTS (SELECT * FROM erc20.weth_hourly_balance_changes WHERE hour >= '2019-03-25' AND hour < '2019-04-01');
SELECT erc20.insert_weth_balance_changes('2019-04-01','2019-04-08') WHERE NOT EXISTS (SELECT * FROM erc20.weth_hourly_balance_changes WHERE hour >= '2019-04-01' AND hour < '2019-04-08');
SELECT erc20.insert_weth_balance_changes('2019-04-08','2019-04-15') WHERE NOT EXISTS (SELECT * FROM erc20.weth_hourly_balance_changes WHERE hour >= '2019-04-08' AND hour < '2019-04-15');
SELECT erc20.insert_weth_balance_changes('2019-04-15','2019-04-22') WHERE NOT EXISTS (SELECT * FROM erc20.weth_hourly_balance_changes WHERE hour >= '2019-04-15' AND hour < '2019-04-22');
SELECT erc20.insert_weth_balance_changes('2019-04-22','2019-04-29') WHERE NOT EXISTS (SELECT * FROM erc20.weth_hourly_balance_changes WHERE hour >= '2019-04-22' AND hour < '2019-04-29');
SELECT erc20.insert_weth_balance_changes('2019-04-29','2019-05-06') WHERE NOT EXISTS (SELECT * FROM erc20.weth_hourly_balance_changes WHERE hour >= '2019-04-29' AND hour < '2019-05-06');
SELECT erc20.insert_weth_balance_changes('2019-05-06','2019-05-13') WHERE NOT EXISTS (SELECT * FROM erc20.weth_hourly_balance_changes WHERE hour >= '2019-05-06' AND hour < '2019-05-13');
SELECT erc20.insert_weth_balance_changes('2019-05-13','2019-05-20') WHERE NOT EXISTS (SELECT * FROM erc20.weth_hourly_balance_changes WHERE hour >= '2019-05-13' AND hour < '2019-05-20');
SELECT erc20.insert_weth_balance_changes('2019-05-20','2019-05-27') WHERE NOT EXISTS (SELECT * FROM erc20.weth_hourly_balance_changes WHERE hour >= '2019-05-20' AND hour < '2019-05-27');
SELECT erc20.insert_weth_balance_changes('2019-05-27','2019-06-03') WHERE NOT EXISTS (SELECT * FROM erc20.weth_hourly_balance_changes WHERE hour >= '2019-05-27' AND hour < '2019-06-03');
SELECT erc20.insert_weth_balance_changes('2019-06-03','2019-06-10') WHERE NOT EXISTS (SELECT * FROM erc20.weth_hourly_balance_changes WHERE hour >= '2019-06-03' AND hour < '2019-06-10');
SELECT erc20.insert_weth_balance_changes('2019-06-10','2019-06-17') WHERE NOT EXISTS (SELECT * FROM erc20.weth_hourly_balance_changes WHERE hour >= '2019-06-10' AND hour < '2019-06-17');
SELECT erc20.insert_weth_balance_changes('2019-06-17','2019-06-24') WHERE NOT EXISTS (SELECT * FROM erc20.weth_hourly_balance_changes WHERE hour >= '2019-06-17' AND hour < '2019-06-24');
SELECT erc20.insert_weth_balance_changes('2019-06-24','2019-07-01') WHERE NOT EXISTS (SELECT * FROM erc20.weth_hourly_balance_changes WHERE hour >= '2019-06-24' AND hour < '2019-07-01');
SELECT erc20.insert_weth_balance_changes('2019-07-01','2019-07-08') WHERE NOT EXISTS (SELECT * FROM erc20.weth_hourly_balance_changes WHERE hour >= '2019-07-01' AND hour < '2019-07-08');
SELECT erc20.insert_weth_balance_changes('2019-07-08','2019-07-15') WHERE NOT EXISTS (SELECT * FROM erc20.weth_hourly_balance_changes WHERE hour >= '2019-07-08' AND hour < '2019-07-15');
SELECT erc20.insert_weth_balance_changes('2019-07-15','2019-07-22') WHERE NOT EXISTS (SELECT * FROM erc20.weth_hourly_balance_changes WHERE hour >= '2019-07-15' AND hour < '2019-07-22');
SELECT erc20.insert_weth_balance_changes('2019-07-22','2019-07-29') WHERE NOT EXISTS (SELECT * FROM erc20.weth_hourly_balance_changes WHERE hour >= '2019-07-22' AND hour < '2019-07-29');
SELECT erc20.insert_weth_balance_changes('2019-07-29','2019-08-05') WHERE NOT EXISTS (SELECT * FROM erc20.weth_hourly_balance_changes WHERE hour >= '2019-07-29' AND hour < '2019-08-05');
SELECT erc20.insert_weth_balance_changes('2019-08-05','2019-08-12') WHERE NOT EXISTS (SELECT * FROM erc20.weth_hourly_balance_changes WHERE hour >= '2019-08-05' AND hour < '2019-08-12');
SELECT erc20.insert_weth_balance_changes('2019-08-12','2019-08-19') WHERE NOT EXISTS (SELECT * FROM erc20.weth_hourly_balance_changes WHERE hour >= '2019-08-12' AND hour < '2019-08-19');
SELECT erc20.insert_weth_balance_changes('2019-08-19','2019-08-26') WHERE NOT EXISTS (SELECT * FROM erc20.weth_hourly_balance_changes WHERE hour >= '2019-08-19' AND hour < '2019-08-26');
SELECT erc20.insert_weth_balance_changes('2019-08-26','2019-09-02') WHERE NOT EXISTS (SELECT * FROM erc20.weth_hourly_balance_changes WHERE hour >= '2019-08-26' AND hour < '2019-09-02');
SELECT erc20.insert_weth_balance_changes('2019-09-02','2019-09-09') WHERE NOT EXISTS (SELECT * FROM erc20.weth_hourly_balance_changes WHERE hour >= '2019-09-02' AND hour < '2019-09-09');
SELECT erc20.insert_weth_balance_changes('2019-09-09','2019-09-16') WHERE NOT EXISTS (SELECT * FROM erc20.weth_hourly_balance_changes WHERE hour >= '2019-09-09' AND hour < '2019-09-16');
SELECT erc20.insert_weth_balance_changes('2019-09-16','2019-09-23') WHERE NOT EXISTS (SELECT * FROM erc20.weth_hourly_balance_changes WHERE hour >= '2019-09-16' AND hour < '2019-09-23');
SELECT erc20.insert_weth_balance_changes('2019-09-23','2019-09-30') WHERE NOT EXISTS (SELECT * FROM erc20.weth_hourly_balance_changes WHERE hour >= '2019-09-23' AND hour < '2019-09-30');
SELECT erc20.insert_weth_balance_changes('2019-09-30','2019-10-07') WHERE NOT EXISTS (SELECT * FROM erc20.weth_hourly_balance_changes WHERE hour >= '2019-09-30' AND hour < '2019-10-07');
SELECT erc20.insert_weth_balance_changes('2019-10-07','2019-10-14') WHERE NOT EXISTS (SELECT * FROM erc20.weth_hourly_balance_changes WHERE hour >= '2019-10-07' AND hour < '2019-10-14');
SELECT erc20.insert_weth_balance_changes('2019-10-14','2019-10-21') WHERE NOT EXISTS (SELECT * FROM erc20.weth_hourly_balance_changes WHERE hour >= '2019-10-14' AND hour < '2019-10-21');
SELECT erc20.insert_weth_balance_changes('2019-10-21','2019-10-28') WHERE NOT EXISTS (SELECT * FROM erc20.weth_hourly_balance_changes WHERE hour >= '2019-10-21' AND hour < '2019-10-28');
SELECT erc20.insert_weth_balance_changes('2019-10-28','2019-11-04') WHERE NOT EXISTS (SELECT * FROM erc20.weth_hourly_balance_changes WHERE hour >= '2019-10-28' AND hour < '2019-11-04');
SELECT erc20.insert_weth_balance_changes('2019-11-04','2019-11-11') WHERE NOT EXISTS (SELECT * FROM erc20.weth_hourly_balance_changes WHERE hour >= '2019-11-04' AND hour < '2019-11-11');
SELECT erc20.insert_weth_balance_changes('2019-11-11','2019-11-18') WHERE NOT EXISTS (SELECT * FROM erc20.weth_hourly_balance_changes WHERE hour >= '2019-11-11' AND hour < '2019-11-18');
SELECT erc20.insert_weth_balance_changes('2019-11-18','2019-11-25') WHERE NOT EXISTS (SELECT * FROM erc20.weth_hourly_balance_changes WHERE hour >= '2019-11-18' AND hour < '2019-11-25');
SELECT erc20.insert_weth_balance_changes('2019-11-25','2019-12-02') WHERE NOT EXISTS (SELECT * FROM erc20.weth_hourly_balance_changes WHERE hour >= '2019-11-25' AND hour < '2019-12-02');
SELECT erc20.insert_weth_balance_changes('2019-12-02','2019-12-09') WHERE NOT EXISTS (SELECT * FROM erc20.weth_hourly_balance_changes WHERE hour >= '2019-12-02' AND hour < '2019-12-09');
SELECT erc20.insert_weth_balance_changes('2019-12-09','2019-12-16') WHERE NOT EXISTS (SELECT * FROM erc20.weth_hourly_balance_changes WHERE hour >= '2019-12-09' AND hour < '2019-12-16');
SELECT erc20.insert_weth_balance_changes('2019-12-16','2019-12-23') WHERE NOT EXISTS (SELECT * FROM erc20.weth_hourly_balance_changes WHERE hour >= '2019-12-16' AND hour < '2019-12-23');
SELECT erc20.insert_weth_balance_changes('2019-12-23','2019-12-30') WHERE NOT EXISTS (SELECT * FROM erc20.weth_hourly_balance_changes WHERE hour >= '2019-12-23' AND hour < '2019-12-30');
SELECT erc20.insert_weth_balance_changes('2019-12-30','2020-01-06') WHERE NOT EXISTS (SELECT * FROM erc20.weth_hourly_balance_changes WHERE hour >= '2019-12-30' AND hour < '2020-01-06');
SELECT erc20.insert_weth_balance_changes('2020-01-06','2020-01-13') WHERE NOT EXISTS (SELECT * FROM erc20.weth_hourly_balance_changes WHERE hour >= '2020-01-06' AND hour < '2020-01-13');
SELECT erc20.insert_weth_balance_changes('2020-01-13','2020-01-20') WHERE NOT EXISTS (SELECT * FROM erc20.weth_hourly_balance_changes WHERE hour >= '2020-01-13' AND hour < '2020-01-20');
SELECT erc20.insert_weth_balance_changes('2020-01-20','2020-01-27') WHERE NOT EXISTS (SELECT * FROM erc20.weth_hourly_balance_changes WHERE hour >= '2020-01-20' AND hour < '2020-01-27');
SELECT erc20.insert_weth_balance_changes('2020-01-27','2020-02-03') WHERE NOT EXISTS (SELECT * FROM erc20.weth_hourly_balance_changes WHERE hour >= '2020-01-27' AND hour < '2020-02-03');
SELECT erc20.insert_weth_balance_changes('2020-02-03','2020-02-10') WHERE NOT EXISTS (SELECT * FROM erc20.weth_hourly_balance_changes WHERE hour >= '2020-02-03' AND hour < '2020-02-10');
SELECT erc20.insert_weth_balance_changes('2020-02-10','2020-02-17') WHERE NOT EXISTS (SELECT * FROM erc20.weth_hourly_balance_changes WHERE hour >= '2020-02-10' AND hour < '2020-02-17');
SELECT erc20.insert_weth_balance_changes('2020-02-17','2020-02-24') WHERE NOT EXISTS (SELECT * FROM erc20.weth_hourly_balance_changes WHERE hour >= '2020-02-17' AND hour < '2020-02-24');
SELECT erc20.insert_weth_balance_changes('2020-02-24','2020-03-02') WHERE NOT EXISTS (SELECT * FROM erc20.weth_hourly_balance_changes WHERE hour >= '2020-02-24' AND hour < '2020-03-02');
SELECT erc20.insert_weth_balance_changes('2020-03-02','2020-03-09') WHERE NOT EXISTS (SELECT * FROM erc20.weth_hourly_balance_changes WHERE hour >= '2020-03-02' AND hour < '2020-03-09');
SELECT erc20.insert_weth_balance_changes('2020-03-09','2020-03-16') WHERE NOT EXISTS (SELECT * FROM erc20.weth_hourly_balance_changes WHERE hour >= '2020-03-09' AND hour < '2020-03-16');
SELECT erc20.insert_weth_balance_changes('2020-03-16','2020-03-23') WHERE NOT EXISTS (SELECT * FROM erc20.weth_hourly_balance_changes WHERE hour >= '2020-03-16' AND hour < '2020-03-23');
SELECT erc20.insert_weth_balance_changes('2020-03-23','2020-03-30') WHERE NOT EXISTS (SELECT * FROM erc20.weth_hourly_balance_changes WHERE hour >= '2020-03-23' AND hour < '2020-03-30');
SELECT erc20.insert_weth_balance_changes('2020-03-30','2020-04-06') WHERE NOT EXISTS (SELECT * FROM erc20.weth_hourly_balance_changes WHERE hour >= '2020-03-30' AND hour < '2020-04-06');
SELECT erc20.insert_weth_balance_changes('2020-04-06','2020-04-13') WHERE NOT EXISTS (SELECT * FROM erc20.weth_hourly_balance_changes WHERE hour >= '2020-04-06' AND hour < '2020-04-13');
SELECT erc20.insert_weth_balance_changes('2020-04-13','2020-04-20') WHERE NOT EXISTS (SELECT * FROM erc20.weth_hourly_balance_changes WHERE hour >= '2020-04-13' AND hour < '2020-04-20');
SELECT erc20.insert_weth_balance_changes('2020-04-20','2020-04-27') WHERE NOT EXISTS (SELECT * FROM erc20.weth_hourly_balance_changes WHERE hour >= '2020-04-20' AND hour < '2020-04-27');
SELECT erc20.insert_weth_balance_changes('2020-04-27','2020-05-04') WHERE NOT EXISTS (SELECT * FROM erc20.weth_hourly_balance_changes WHERE hour >= '2020-04-27' AND hour < '2020-05-04');
SELECT erc20.insert_weth_balance_changes('2020-05-04','2020-05-11') WHERE NOT EXISTS (SELECT * FROM erc20.weth_hourly_balance_changes WHERE hour >= '2020-05-04' AND hour < '2020-05-11');
SELECT erc20.insert_weth_balance_changes('2020-05-11','2020-05-18') WHERE NOT EXISTS (SELECT * FROM erc20.weth_hourly_balance_changes WHERE hour >= '2020-05-11' AND hour < '2020-05-18');
SELECT erc20.insert_weth_balance_changes('2020-05-18','2020-05-25') WHERE NOT EXISTS (SELECT * FROM erc20.weth_hourly_balance_changes WHERE hour >= '2020-05-18' AND hour < '2020-05-25');
SELECT erc20.insert_weth_balance_changes('2020-05-25','2020-06-01') WHERE NOT EXISTS (SELECT * FROM erc20.weth_hourly_balance_changes WHERE hour >= '2020-05-25' AND hour < '2020-06-01');
SELECT erc20.insert_weth_balance_changes('2020-06-01','2020-06-08') WHERE NOT EXISTS (SELECT * FROM erc20.weth_hourly_balance_changes WHERE hour >= '2020-06-01' AND hour < '2020-06-08');
SELECT erc20.insert_weth_balance_changes('2020-06-08','2020-06-15') WHERE NOT EXISTS (SELECT * FROM erc20.weth_hourly_balance_changes WHERE hour >= '2020-06-08' AND hour < '2020-06-15');
SELECT erc20.insert_weth_balance_changes('2020-06-15','2020-06-22') WHERE NOT EXISTS (SELECT * FROM erc20.weth_hourly_balance_changes WHERE hour >= '2020-06-15' AND hour < '2020-06-22');
SELECT erc20.insert_weth_balance_changes('2020-06-22','2020-06-29') WHERE NOT EXISTS (SELECT * FROM erc20.weth_hourly_balance_changes WHERE hour >= '2020-06-22' AND hour < '2020-06-29');
SELECT erc20.insert_weth_balance_changes('2020-06-29','2020-07-06') WHERE NOT EXISTS (SELECT * FROM erc20.weth_hourly_balance_changes WHERE hour >= '2020-06-29' AND hour < '2020-07-06');
SELECT erc20.insert_weth_balance_changes('2020-07-06','2020-07-13') WHERE NOT EXISTS (SELECT * FROM erc20.weth_hourly_balance_changes WHERE hour >= '2020-07-06' AND hour < '2020-07-13');
SELECT erc20.insert_weth_balance_changes('2020-07-13','2020-07-20') WHERE NOT EXISTS (SELECT * FROM erc20.weth_hourly_balance_changes WHERE hour >= '2020-07-13' AND hour < '2020-07-20');
SELECT erc20.insert_weth_balance_changes('2020-07-20','2020-07-27') WHERE NOT EXISTS (SELECT * FROM erc20.weth_hourly_balance_changes WHERE hour >= '2020-07-20' AND hour < '2020-07-27');
SELECT erc20.insert_weth_balance_changes('2020-07-27','2020-08-03') WHERE NOT EXISTS (SELECT * FROM erc20.weth_hourly_balance_changes WHERE hour >= '2020-07-27' AND hour < '2020-08-03');
SELECT erc20.insert_weth_balance_changes('2020-08-03','2020-08-10') WHERE NOT EXISTS (SELECT * FROM erc20.weth_hourly_balance_changes WHERE hour >= '2020-08-03' AND hour < '2020-08-10');
SELECT erc20.insert_weth_balance_changes('2020-08-10','2020-08-17') WHERE NOT EXISTS (SELECT * FROM erc20.weth_hourly_balance_changes WHERE hour >= '2020-08-10' AND hour < '2020-08-17');
SELECT erc20.insert_weth_balance_changes('2020-08-17','2020-08-24') WHERE NOT EXISTS (SELECT * FROM erc20.weth_hourly_balance_changes WHERE hour >= '2020-08-17' AND hour < '2020-08-24');
SELECT erc20.insert_weth_balance_changes('2020-08-24','2020-08-31') WHERE NOT EXISTS (SELECT * FROM erc20.weth_hourly_balance_changes WHERE hour >= '2020-08-24' AND hour < '2020-08-31');
SELECT erc20.insert_weth_balance_changes('2020-08-31','2020-09-07') WHERE NOT EXISTS (SELECT * FROM erc20.weth_hourly_balance_changes WHERE hour >= '2020-08-31' AND hour < '2020-09-07');
SELECT erc20.insert_weth_balance_changes('2020-09-07','2020-09-14') WHERE NOT EXISTS (SELECT * FROM erc20.weth_hourly_balance_changes WHERE hour >= '2020-09-07' AND hour < '2020-09-14');
SELECT erc20.insert_weth_balance_changes('2020-09-14','2020-09-21') WHERE NOT EXISTS (SELECT * FROM erc20.weth_hourly_balance_changes WHERE hour >= '2020-09-14' AND hour < '2020-09-21');
SELECT erc20.insert_weth_balance_changes('2020-09-21','2020-09-28') WHERE NOT EXISTS (SELECT * FROM erc20.weth_hourly_balance_changes WHERE hour >= '2020-09-21' AND hour < '2020-09-28');
SELECT erc20.insert_weth_balance_changes('2020-09-28','2020-10-05') WHERE NOT EXISTS (SELECT * FROM erc20.weth_hourly_balance_changes WHERE hour >= '2020-09-28' AND hour < '2020-10-05');
SELECT erc20.insert_weth_balance_changes('2020-10-05','2020-10-12') WHERE NOT EXISTS (SELECT * FROM erc20.weth_hourly_balance_changes WHERE hour >= '2020-10-05' AND hour < '2020-10-12');
SELECT erc20.insert_weth_balance_changes('2020-10-12','2020-10-19') WHERE NOT EXISTS (SELECT * FROM erc20.weth_hourly_balance_changes WHERE hour >= '2020-10-12' AND hour < '2020-10-19');
SELECT erc20.insert_weth_balance_changes('2020-10-19','2020-10-26') WHERE NOT EXISTS (SELECT * FROM erc20.weth_hourly_balance_changes WHERE hour >= '2020-10-19' AND hour < '2020-10-26');
SELECT erc20.insert_weth_balance_changes('2020-10-26','2020-11-02') WHERE NOT EXISTS (SELECT * FROM erc20.weth_hourly_balance_changes WHERE hour >= '2020-10-26' AND hour < '2020-11-02');
SELECT erc20.insert_weth_balance_changes('2020-11-02','2020-11-09') WHERE NOT EXISTS (SELECT * FROM erc20.weth_hourly_balance_changes WHERE hour >= '2020-11-02' AND hour < '2020-11-09');
SELECT erc20.insert_weth_balance_changes('2020-11-09','2020-11-16') WHERE NOT EXISTS (SELECT * FROM erc20.weth_hourly_balance_changes WHERE hour >= '2020-11-09' AND hour < '2020-11-16');
SELECT erc20.insert_weth_balance_changes('2020-11-16','2020-11-23') WHERE NOT EXISTS (SELECT * FROM erc20.weth_hourly_balance_changes WHERE hour >= '2020-11-16' AND hour < '2020-11-23');
SELECT erc20.insert_weth_balance_changes('2020-11-23','2020-11-30') WHERE NOT EXISTS (SELECT * FROM erc20.weth_hourly_balance_changes WHERE hour >= '2020-11-23' AND hour < '2020-11-30');
SELECT erc20.insert_weth_balance_changes('2020-11-30','2020-12-07') WHERE NOT EXISTS (SELECT * FROM erc20.weth_hourly_balance_changes WHERE hour >= '2020-11-30' AND hour < '2020-12-07');
SELECT erc20.insert_weth_balance_changes('2020-12-07','2020-12-14') WHERE NOT EXISTS (SELECT * FROM erc20.weth_hourly_balance_changes WHERE hour >= '2020-12-07' AND hour < '2020-12-14');
SELECT erc20.insert_weth_balance_changes('2020-12-14','2020-12-21') WHERE NOT EXISTS (SELECT * FROM erc20.weth_hourly_balance_changes WHERE hour >= '2020-12-14' AND hour < '2020-12-21');
SELECT erc20.insert_weth_balance_changes('2020-12-21','2020-12-28') WHERE NOT EXISTS (SELECT * FROM erc20.weth_hourly_balance_changes WHERE hour >= '2020-12-21' AND hour < '2020-12-28');
SELECT erc20.insert_weth_balance_changes('2020-12-28','2021-01-04') WHERE NOT EXISTS (SELECT * FROM erc20.weth_hourly_balance_changes WHERE hour >= '2020-12-28' AND hour < '2021-01-04');
SELECT erc20.insert_weth_balance_changes('2021-01-04','2021-01-11') WHERE NOT EXISTS (SELECT * FROM erc20.weth_hourly_balance_changes WHERE hour >= '2021-01-04' AND hour < '2021-01-11');
SELECT erc20.insert_weth_balance_changes('2021-01-11','2021-01-18') WHERE NOT EXISTS (SELECT * FROM erc20.weth_hourly_balance_changes WHERE hour >= '2021-01-11' AND hour < '2021-01-18');
SELECT erc20.insert_weth_balance_changes('2021-01-18','2021-01-25') WHERE NOT EXISTS (SELECT * FROM erc20.weth_hourly_balance_changes WHERE hour >= '2021-01-18' AND hour < '2021-01-25');
SELECT erc20.insert_weth_balance_changes('2021-01-25','2021-02-01') WHERE NOT EXISTS (SELECT * FROM erc20.weth_hourly_balance_changes WHERE hour >= '2021-01-25' AND hour < '2021-02-01');
SELECT erc20.insert_weth_balance_changes('2021-02-01','2021-02-08') WHERE NOT EXISTS (SELECT * FROM erc20.weth_hourly_balance_changes WHERE hour >= '2021-02-01' AND hour < '2021-02-08');
SELECT erc20.insert_weth_balance_changes('2021-02-08','2021-02-15') WHERE NOT EXISTS (SELECT * FROM erc20.weth_hourly_balance_changes WHERE hour >= '2021-02-08' AND hour < '2021-02-15');
SELECT erc20.insert_weth_balance_changes('2021-02-15','2021-02-22') WHERE NOT EXISTS (SELECT * FROM erc20.weth_hourly_balance_changes WHERE hour >= '2021-02-15' AND hour < '2021-02-22');
SELECT erc20.insert_weth_balance_changes('2021-02-22','2021-03-01') WHERE NOT EXISTS (SELECT * FROM erc20.weth_hourly_balance_changes WHERE hour >= '2021-02-22' AND hour < '2021-03-01');
SELECT erc20.insert_weth_balance_changes('2021-03-01','2021-03-08') WHERE NOT EXISTS (SELECT * FROM erc20.weth_hourly_balance_changes WHERE hour >= '2021-03-01' AND hour < '2021-03-08');
SELECT erc20.insert_weth_balance_changes('2021-03-08','2021-03-15') WHERE NOT EXISTS (SELECT * FROM erc20.weth_hourly_balance_changes WHERE hour >= '2021-03-08' AND hour < '2021-03-15');
SELECT erc20.insert_weth_balance_changes('2021-03-15','2021-03-22') WHERE NOT EXISTS (SELECT * FROM erc20.weth_hourly_balance_changes WHERE hour >= '2021-03-15' AND hour < '2021-03-22');
SELECT erc20.insert_weth_balance_changes('2021-03-22','2021-03-29') WHERE NOT EXISTS (SELECT * FROM erc20.weth_hourly_balance_changes WHERE hour >= '2021-03-22' AND hour < '2021-03-29');
SELECT erc20.insert_weth_balance_changes('2021-03-29','2021-04-05') WHERE NOT EXISTS (SELECT * FROM erc20.weth_hourly_balance_changes WHERE hour >= '2021-03-29' AND hour < '2021-04-05');
SELECT erc20.insert_weth_balance_changes('2021-04-05','2021-04-12') WHERE NOT EXISTS (SELECT * FROM erc20.weth_hourly_balance_changes WHERE hour >= '2021-04-05' AND hour < '2021-04-12');
SELECT erc20.insert_weth_balance_changes('2021-04-12','2021-04-19') WHERE NOT EXISTS (SELECT * FROM erc20.weth_hourly_balance_changes WHERE hour >= '2021-04-12' AND hour < '2021-04-19');
SELECT erc20.insert_weth_balance_changes('2021-04-19','2021-04-26') WHERE NOT EXISTS (SELECT * FROM erc20.weth_hourly_balance_changes WHERE hour >= '2021-04-19' AND hour < '2021-04-26');
SELECT erc20.insert_weth_balance_changes('2021-04-26','2021-05-03') WHERE NOT EXISTS (SELECT * FROM erc20.weth_hourly_balance_changes WHERE hour >= '2021-04-26' AND hour < '2021-05-03');
SELECT erc20.insert_weth_balance_changes('2021-05-03','2021-05-10') WHERE NOT EXISTS (SELECT * FROM erc20.weth_hourly_balance_changes WHERE hour >= '2021-05-03' AND hour < '2021-05-10');
SELECT erc20.insert_weth_balance_changes('2021-05-10','2021-05-17') WHERE NOT EXISTS (SELECT * FROM erc20.weth_hourly_balance_changes WHERE hour >= '2021-05-10' AND hour < '2021-05-17');
SELECT erc20.insert_weth_balance_changes('2021-05-17','2021-05-24') WHERE NOT EXISTS (SELECT * FROM erc20.weth_hourly_balance_changes WHERE hour >= '2021-05-17' AND hour < '2021-05-24');
SELECT erc20.insert_weth_balance_changes('2021-05-24','2021-05-31') WHERE NOT EXISTS (SELECT * FROM erc20.weth_hourly_balance_changes WHERE hour >= '2021-05-24' AND hour < '2021-05-31');
SELECT erc20.insert_weth_balance_changes('2021-05-31','2021-06-07') WHERE NOT EXISTS (SELECT * FROM erc20.weth_hourly_balance_changes WHERE hour >= '2021-05-31' AND hour < '2021-06-07');
SELECT erc20.insert_weth_balance_changes('2021-06-07','2021-06-14') WHERE NOT EXISTS (SELECT * FROM erc20.weth_hourly_balance_changes WHERE hour >= '2021-06-07' AND hour < '2021-06-14');
SELECT erc20.insert_weth_balance_changes('2021-06-14','2021-06-21') WHERE NOT EXISTS (SELECT * FROM erc20.weth_hourly_balance_changes WHERE hour >= '2021-06-14' AND hour < '2021-06-21');
SELECT erc20.insert_weth_balance_changes('2021-06-21','2021-06-28') WHERE NOT EXISTS (SELECT * FROM erc20.weth_hourly_balance_changes WHERE hour >= '2021-06-21' AND hour < '2021-06-28');
SELECT erc20.insert_weth_balance_changes('2021-06-28','2021-07-05') WHERE NOT EXISTS (SELECT * FROM erc20.weth_hourly_balance_changes WHERE hour >= '2021-06-28' AND hour < '2021-07-05');
SELECT erc20.insert_weth_balance_changes('2021-07-05','2021-07-12') WHERE NOT EXISTS (SELECT * FROM erc20.weth_hourly_balance_changes WHERE hour >= '2021-07-05' AND hour < '2021-07-12');
SELECT erc20.insert_weth_balance_changes('2021-07-12','2021-07-19') WHERE NOT EXISTS (SELECT * FROM erc20.weth_hourly_balance_changes WHERE hour >= '2021-07-12' AND hour < '2021-07-19');
SELECT erc20.insert_weth_balance_changes('2021-07-19','2021-07-26') WHERE NOT EXISTS (SELECT * FROM erc20.weth_hourly_balance_changes WHERE hour >= '2021-07-19' AND hour < '2021-07-26');
SELECT erc20.insert_weth_balance_changes('2021-07-26','2021-08-02') WHERE NOT EXISTS (SELECT * FROM erc20.weth_hourly_balance_changes WHERE hour >= '2021-07-26' AND hour < '2021-08-02');
SELECT erc20.insert_weth_balance_changes('2021-08-02','2021-08-09') WHERE NOT EXISTS (SELECT * FROM erc20.weth_hourly_balance_changes WHERE hour >= '2021-08-02' AND hour < '2021-08-09');
SELECT erc20.insert_weth_balance_changes('2021-08-09','2021-08-16') WHERE NOT EXISTS (SELECT * FROM erc20.weth_hourly_balance_changes WHERE hour >= '2021-08-09' AND hour < '2021-08-16');
SELECT erc20.insert_weth_balance_changes('2021-08-16','2021-08-23') WHERE NOT EXISTS (SELECT * FROM erc20.weth_hourly_balance_changes WHERE hour >= '2021-08-16' AND hour < '2021-08-23');
SELECT erc20.insert_weth_balance_changes('2021-08-23','2021-08-30') WHERE NOT EXISTS (SELECT * FROM erc20.weth_hourly_balance_changes WHERE hour >= '2021-08-23' AND hour < '2021-08-30');
SELECT erc20.insert_weth_balance_changes('2021-08-30','2021-09-06') WHERE NOT EXISTS (SELECT * FROM erc20.weth_hourly_balance_changes WHERE hour >= '2021-08-30' AND hour < '2021-09-06');
SELECT erc20.insert_weth_balance_changes('2021-09-06','2021-09-13') WHERE NOT EXISTS (SELECT * FROM erc20.weth_hourly_balance_changes WHERE hour >= '2021-09-06' AND hour < '2021-09-13');
SELECT erc20.insert_weth_balance_changes('2021-09-13','2021-09-20') WHERE NOT EXISTS (SELECT * FROM erc20.weth_hourly_balance_changes WHERE hour >= '2021-09-13' AND hour < '2021-09-20');
SELECT erc20.insert_weth_balance_changes('2021-09-20','2021-09-27') WHERE NOT EXISTS (SELECT * FROM erc20.weth_hourly_balance_changes WHERE hour >= '2021-09-20' AND hour < '2021-09-27');
SELECT erc20.insert_weth_balance_changes('2021-09-27','2021-10-04') WHERE NOT EXISTS (SELECT * FROM erc20.weth_hourly_balance_changes WHERE hour >= '2021-09-27' AND hour < '2021-10-04');

-- final fill
SELECT erc20.insert_weth_balance_changes(
    '2021-10-04',
    date_trunc('hour', now())
)
WHERE NOT EXISTS (
    SELECT *
    FROM erc20.weth_hourly_balance_changes
    WHERE hour >= '2021-10-04'
    AND hour < date_trunc('hour', now())
);


INSERT INTO cron.job (schedule, command)
VALUES ('57 * * * *', $$
    SELECT erc20.insert_weth_balance_changes(
        (SELECT (SELECT max(hour) FROM erc20.weth_hourly_balance_changes) - interval '24 hours'),
        (SELECT date_trunc('hour', now())));
$$)
ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;
