BEGIN;
DROP TABLE IF EXISTS zeroex.view_api_affiliate_data;
CREATE TABLE IF NOT EXISTS zeroex.view_api_affiliate_data (
    tx_hash BYTEA
    , trace_address INTEGER[]
    , block_number BIGINT
    , block_time TIMESTAMPTZ
    , caller BYTEA
    , callee BYTEA
    , affiliate_address BYTEA
    , quote_timestamp BIGINT
    , UNIQUE(tx_hash, trace_address)
);

CREATE INDEX IF NOT EXISTS api_affiliate_tx_index ON zeroex.view_api_affiliate_data (tx_hash);
CREATE INDEX IF NOT EXISTS api_affiliate_timestamp_index ON zeroex.view_api_affiliate_data (block_time);
CREATE INDEX IF NOT EXISTS api_affiliate_affiliate_index ON zeroex.view_api_affiliate_data (affiliate_address);

CREATE OR REPLACE FUNCTION zeroex.insert_api_data(start_ts timestamptz, end_ts timestamptz=now()) RETURNS integer
LANGUAGE plpgsql AS $function$
DECLARE r integer;
BEGIN
WITH rows AS (
    INSERT INTO zeroex.view_api_affiliate_data
    SELECT
        tr.tx_hash
        , tr.trace_address
        , tr.block_number
        , tr.block_time
        , "from" AS caller
        , "to" AS callee
        , CASE
                WHEN POSITION('\x869584cd'::BYTEA IN input) <> 0 THEN SUBSTRING(input from (position('\x869584cd'::BYTEA IN input) + 16) for 20)
                WHEN POSITION('\xfbc019a7'::BYTEA IN input) <> 0 THEN SUBSTRING(input from (position('\xfbc019a7'::BYTEA IN input) + 16) for 20)
            END AS affiliate_address
        , NULL AS quote_timestamp
    FROM ethereum."traces" tr
    WHERE
        tr."to" IN (
            -- exchange contract
            '\x61935cbdd02287b511119ddb11aeb42f1593b7ef'::BYTEA
            -- forwarder addresses
            , '\x6958f5e95332d93d21af0d7b9ca85b8212fee0a5'::BYTEA
            , '\x4aa817c6f383c8e8ae77301d18ce48efb16fd2be'::BYTEA
            , '\x4ef40d1bf0983899892946830abf99eca2dbc5ce'::BYTEA
            -- exchange proxy
            , '\xdef1c0ded9bec7f1a1670819833240f027b25eff'::BYTEA
        )
    AND (
        POSITION('\x869584cd'::BYTEA IN input) <> 0
        OR POSITION('\xfbc019a7'::BYTEA IN input) <> 0
    )
    AND tr.block_time >= start_ts
    AND tr.block_time < end_ts
    ON CONFLICT DO NOTHING
    RETURNING 1
)
SELECT count(*) INTO r FROM rows;
RETURN r;
END
$function$;

SELECT zeroex.insert_api_data('2017-01-01', now()) WHERE NOT EXISTS (SELECT * FROM zeroex.view_api_affiliate_data);

INSERT INTO cron.job (schedule, command)
VALUES ('*/15 * * * *',  $$SELECT zeroex.insert_api_data((SELECT max(block_time) from zeroex.view_api_affiliate_data) - interval '1 day', now())$$)
ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;
COMMIT;
