DROP FUNCTION loopring.fn_process_trade_block;
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

CREATE OR REPLACE FUNCTION loopring.fn_process_trade_block(blockSize integer, data bytea, block_timestamp timestamptz) RETURNS trade_struct[] AS $$
DECLARE
    i integer;
    offsetTokens integer;
    offsetFills integer;
    offsetAccounts integer;
    protocolFeeTakerBips double precision;
    protocolFeeMakerBips double precision;
    tokenA integer;
    symbolA varchar;
    tokenB integer;
    fillA bytea;
    fillB bytea;
    accounts_bytes bytea;
    accountA integer;
    accountB integer;
    trade trade_struct;
    trades trade_struct[];
BEGIN
    offsetTokens = 111 + blockSize * 10;
    offsetFills = 111 + blockSize * 12;
    offsetAccounts = 111 + blockSize * 5;

    protocolFeeTakerBips = get_byte(data, 73);
    protocolFeeMakerBips = get_byte(data, 74);
    protocolFeeTakerBips = protocolFeeTakerBips / 100000;
    protocolFeeMakerBips = protocolFeeMakerBips / 100000;

    FOR i IN 1 .. blockSize
    LOOP
        fillA = substr(data, offsetFills, 3);
        fillB = substr(data, offsetFills + 3, 3);

        tokenA = get_byte(substr(data, offsetTokens, 1), 0);
        tokenB = get_byte(substr(data, offsetTokens + 1, 1), 0);

        accounts_bytes = substr(data, offsetAccounts, 5);
        accountA = (get_byte(accounts_bytes, 0) * 65536 + get_byte(accounts_bytes, 1) * 256 + get_byte(accounts_bytes, 2)) / 16;
        accountB = (get_byte(accounts_bytes, 2) * 65536 + get_byte(accounts_bytes, 3) * 256 + get_byte(accounts_bytes, 4)) & (1048576 - 1);

        SELECT block_timestamp,
               tokenA,
               loopring.fn_decode_float(fillA) as fillA,
               tokenB,
               loopring.fn_decode_float(fillB) as fillB,
               accountA,
               accountB,
               protocolFeeTakerBips,
               protocolFeeMakerBips
        INTO trade;
        trades = array_append(trades, trade);

        offsetFills = offsetFills + 6;
        offsetTokens = offsetTokens + 2;
        offsetAccounts = offsetAccounts + 5;
    END LOOP;

    RETURN trades;
END; $$
LANGUAGE PLPGSQL;