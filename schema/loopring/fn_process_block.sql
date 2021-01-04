DROP FUNCTION loopring.fn_process_block;

DROP TYPE loopring.transaction_struct;
DROP TYPE loopring.deposit_struct;
DROP TYPE loopring.withdraw_struct;
DROP TYPE loopring.transfer_struct;
DROP TYPE loopring.spot_trade_struct;
DROP TYPE loopring.account_update_struct;
DROP TYPE loopring.amm_update_struct;
DROP TYPE loopring.signature_verification_struct;

CREATE TYPE loopring.deposit_struct AS (
    toAddress bytea,
    toAccount integer,
    token integer,
    amount double precision
);

CREATE TYPE loopring.withdraw_struct AS (
    fromAddress bytea,
    fromAccount integer,
    token integer,
    amount double precision,
    feeToken integer,
    fee double precision
);

CREATE TYPE loopring.transfer_struct AS (
    token integer,
    amount double precision,
    feeToken integer,
    fee double precision,
    fromAccount integer,
    toAccount integer,
    toAddress bytea,
    fromAddress bytea
);

CREATE TYPE loopring.spot_trade_struct AS (
    accountA integer,
    accountB integer,
    tokenA integer,
    tokenB integer,
    amountA double precision,
    amountB double precision
);

CREATE TYPE loopring.account_update_struct AS (
    ownerAddress bytea,
    ownerAccount integer,
    feeToken integer,
    fee double precision,
    publicKey bytea
);

CREATE TYPE loopring.amm_update_struct AS (
    ownerAddress bytea,
    ownerAccount integer,
    token integer,
    feeBips integer,
    weight double precision
);

CREATE TYPE loopring.signature_verification_struct AS (
    ownerAddress bytea,
    ownerAccount integer
);

CREATE TYPE loopring.transaction_struct AS (
    block_timestamp timestamptz,
    blockIdx integer,
    txIdx integer,
    txType integer,
    deposit loopring.deposit_struct,
    withdraw loopring.withdraw_struct,
    transfer loopring.transfer_struct,
    spot_trade loopring.spot_trade_struct,
    account_update loopring.account_update_struct,
    amm_update loopring.amm_update_struct,
    signature_verification loopring.signature_verification_struct
);


CREATE OR REPLACE FUNCTION loopring.fn_process_block(blockSize integer, _data bytea, block_timestamp timestamptz, blockIdx integer)
RETURNS loopring.transaction_struct[]
AS $$
DECLARE
    data bytea;
    i integer;
    transaction loopring.transaction_struct;
    transactions loopring.transaction_struct[];
BEGIN

    FOR i IN 0 .. blockSize-1
    LOOP
        data = (substr(_data, 99+i*29, 29) || substr(_data, 99+blockSize*29+i*39, 39));

        SELECT
            block_timestamp,
            blockIdx,
            i,
            get_byte(substr(data, 1, 1), 0) as txType,
            (
                substr(data, 2, 20),
                loopring.fn_to_uint32(substr(data, 2 + 20, 4)),
                loopring.fn_to_uint16(substr(data, 2 + 20 + 4, 2)),
                loopring.fn_to_uint96(substr(data, 2 + 20 + 4 + 2, 12))
            ) as deposit,
            (
                substr(data, 2 + 1, 20),
                loopring.fn_to_uint32(substr(data, 2 + 1 + 20, 4)),
                loopring.fn_to_uint16(substr(data, 2 + 1 + 20 + 4, 2)),
                loopring.fn_to_uint96(substr(data, 2 + 1 + 20 + 4 + 2, 12)),
                loopring.fn_to_uint16(substr(data, 2 + 1 + 20 + 4 + 2 + 12, 2)),
                loopring.fn_decode_float_16(substr(data, 2 + 1 + 20 + 4 + 2 + 12 + 2, 2))
            ) as withdraw,
            (
                loopring.fn_to_uint16(substr(data, 2 + 1 + 4 + 4, 2)),
                loopring.fn_decode_float_24(substr(data, 2 + 1 + 4 + 4 + 2, 3)),
                loopring.fn_to_uint16(substr(data, 2 + 1 + 4 + 4 + 2 + 3, 2)),
                loopring.fn_decode_float_16(substr(data, 2 + 1 + 4 + 4 + 2 + 3 + 2, 2)),
                loopring.fn_to_uint32(substr(data, 2 + 1, 4)),
                loopring.fn_to_uint32(substr(data, 2 + 1 + 4, 4)),
                substr(data, 2 + 1 + 4 + 4 + 2 + 3 + 2 + 2 + 4, 20),
                substr(data, 2 + 1 + 4 + 4 + 2 + 3 + 2 + 2 + 4 + 20, 20)
            ) as transfer,
            (
                loopring.fn_to_uint32(substr(data, 2 + 4 + 4, 4)),
                loopring.fn_to_uint32(substr(data, 2 + 4 + 4 + 4, 4)),
                loopring.fn_to_uint16(substr(data, 2 + 4 + 4 + 4 + 4, 2)),
                loopring.fn_to_uint16(substr(data, 2 + 4 + 4 + 4 + 4 + 2, 2)),
                loopring.fn_decode_float_24(substr(data, 2 + 4 + 4 + 4 + 4 + 2 + 2, 3)),
                loopring.fn_decode_float_24(substr(data, 2 + 4 + 4 + 4 + 4 + 2 + 2 + 3, 3))
            ) as spot_trade,
            (
                substr(data, 2 + 1, 20),
                loopring.fn_to_uint32(substr(data, 2 + 1 + 20, 4)),
                loopring.fn_to_uint16(substr(data, 2 + 1 + 20 + 4, 2)),
                loopring.fn_decode_float_16(substr(data, 2 + 1 + 20 + 4 + 2, 2)),
                substr(data, 2 + 1 + 20 + 4 + 2 + 2, 32)
            ) as account_update,
            (
                substr(data, 2, 20),
                loopring.fn_to_uint32(substr(data, 2 + 20, 4)),
                loopring.fn_to_uint16(substr(data, 2 + 20 + 4, 2)),
                get_byte(substr(data, 2 + 20 + 4 + 2, 1), 0),
                loopring.fn_to_uint96(substr(data, 2 + 20 + 4 + 2 + 1, 12))
            ) as amm_update,
            (
                substr(data, 2, 20),
                loopring.fn_to_uint32(substr(data, 2 + 20, 4))
            ) as signature_verification
        INTO transaction;
        transactions = array_append(transactions, transaction);

    END LOOP;

    RETURN transactions;
END;
$$
LANGUAGE PLPGSQL;