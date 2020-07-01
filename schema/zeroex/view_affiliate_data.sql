BEGIN;
CREATE TABLE IF NOT EXISTS zeroex.view_api_affiliate_data (
    tx_hash BYTEA
    , block_number BIGINT
    , block_time TIMESTAMPTZ
    , caller BYTEA
    , callee BYTEA
    , affiliate_address BYTEA
    , quote_timestamp BIGINT
);

CREATE INDEX IF NOT EXISTS api_affiliate_tx_index ON zeroex.view_api_affiliate_data (tx_hash);
CREATE INDEX IF NOT EXISTS api_affiliate_timestamp_index ON zeroex.view_api_affiliate_data (block_time);
CREATE INDEX IF NOT EXISTS api_affiliate_affiliate_index ON zeroex.view_api_affiliate_data (affiliate_address);

SELECT cron.schedule('*/10 * * * *', $$
    INSERT INTO zeroex.view_api_affiliate_data (
        SELECT
            tr.tx_hash
            , tr.block_number
            , tr.block_time
            , "from" AS caller
            , "to" AS callee
            , substring(input from (position('\x869584cd'::BYTEA IN input) + 16) for 20) AS affiliate_address
            , HEX_TO_INT(RIGHT(substring(input from (position('\x869584cd'::BYTEA IN input) + 36) for 32)::VARCHAR,8)) AS quote_timestamp
        FROM ethereum."traces" tr
        WHERE
            tr."to" IN (
                -- exchange contract
                '\x61935cbdd02287b511119ddb11aeb42f1593b7ef'::BYTEA
                -- forwarder addresses
                , '\x6958f5e95332d93d21af0d7b9ca85b8212fee0a5'::BYTEA
                , '\x4aa817c6f383c8e8ae77301d18ce48efb16fd2be'::BYTEA
                , '\x4ef40d1bf0983899892946830abf99eca2dbc5ce'::BYTEA
            )
            AND position('\x869584cd'::BYTEA IN input) <> 0
            AND tr.block_time > (SELECT COALESCE(MAX(block_time), '2020-02-25'::TIMESTAMP) FROM zeroex.view_api_affiliate_data)
            AND tr.block_time < ((SELECT COALESCE(MAX(block_time), '2020-02-25'::TIMESTAMP) FROM zeroex.view_api_affiliate_data) + '7 days'::INTERVAL)
            AND tr.block_time < (CURRENT_TIMESTAMP - '3 minutes'::INTERVAL)
    );
$$);
COMMIT;
