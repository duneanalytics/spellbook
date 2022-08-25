DROP FUNCTION loopring.fn_process_trade_block_v1;

DROP TYPE loopring.trade_struct;

CREATE TYPE loopring.trade_struct AS (
    block_timestamp timestamptz,
    tokenA integer,
    fillA double precision,
    tokenB integer,
    fillB double precision,
    accountA integer,
    accountB integer,
    protocolFeeTakerBips double precision,
    protocolFeeMakerBips double precision
);

CREATE OR REPLACE FUNCTION loopring.fn_process_trade_block_v1 (blockSize integer, data bytea, block_timestamp timestamptz)
    RETURNS SETOF loopring.trade_struct
    AS $$
DECLARE
    offsetTokens integer;
    offsetFills integer;
    offsetAccounts integer;
    protocolFeeTakerBips double precision;
    protocolFeeMakerBips double precision;
BEGIN
    offsetTokens = 111 + blockSize * 10;
    offsetFills = 111 + blockSize * 12;
    offsetAccounts = 111 + blockSize * 5;
    protocolFeeTakerBips = get_byte(data, 73)::numeric / 100000;
    protocolFeeMakerBips = get_byte(data, 74)::numeric / 100000;
    RETURN query
    SELECT
        block_timestamp,
        get_byte(substr(data, (offsetTokens + i * 2), 1), 0),
        loopring.fn_decode_float_24(substr(data, (offsetFills + i * 6), 3)),
        get_byte(substr(data, (offsetTokens + i * 2) + 1, 1), 0),
        loopring.fn_decode_float_24(substr(data, (offsetFills + i * 6) + 3, 3)),
        (get_byte(substr(data, offsetAccounts + i * 5, 5), 0) * 65536 + get_byte(substr(data, offsetAccounts + i * 5, 5), 1) * 256 + get_byte(substr(data, offsetAccounts + i * 5, 5), 2)) / 16, (get_byte(substr(data, offsetAccounts + i * 5, 5), 2) * 65536 + get_byte(substr(data, offsetAccounts + i * 5, 5), 3) * 256 + get_byte(substr(data, offsetAccounts + i * 5, 5), 4)) & (1048576 - 1),
        protocolFeeTakerBips,
        protocolFeeMakerBips
    FROM
        generate_series(0, blockSize - 1) i;
END;
$$
LANGUAGE PLPGSQL;

