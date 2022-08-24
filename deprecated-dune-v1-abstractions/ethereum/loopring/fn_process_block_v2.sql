DROP FUNCTION loopring.fn_process_block_v2;

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
    toAccount numeric,
    token numeric,
    amount double precision
);

CREATE TYPE loopring.withdraw_struct AS (
    fromAddress bytea,
    fromAccount numeric,
    token numeric,
    amount double precision,
    feeToken numeric,
    fee double precision
);

CREATE TYPE loopring.transfer_struct AS (
    token numeric,
    amount double precision,
    feeToken numeric,
    fee double precision,
    fromAccount numeric,
    toAccount numeric,
    toAddress bytea,
    fromAddress bytea
);

CREATE TYPE loopring.spot_trade_struct AS (
    accountA numeric,
    accountB numeric,
    tokenA numeric,
    tokenB numeric,
    amountA double precision,
    amountB double precision
);

CREATE TYPE loopring.account_update_struct AS (
    ownerAddress bytea,
    ownerAccount numeric,
    feeToken numeric,
    fee double precision,
    publicKey bytea
);

CREATE TYPE loopring.amm_update_struct AS (
    ownerAddress bytea,
    ownerAccount numeric,
    token numeric,
    feeBips numeric,
    weight double precision
);

CREATE TYPE loopring.signature_verification_struct AS (
    ownerAddress bytea,
    ownerAccount numeric
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


CREATE OR REPLACE FUNCTION loopring.fn_process_block_v2(blockSize integer, _data bytea, block_timestamp timestamptz, blockIdx integer)
RETURNS SETOF loopring.transaction_struct
AS $$
BEGIN
RETURN QUERY
SELECT
    block_timestamp,
    blockIdx,
    i,
    get_byte(substr(x.data, 1, 1), 0) as txType,
    (
        substr(x.data, 2, 20),
        bytea2numeric(substr(x.data, 2 + 20, 4), false),
        bytea2numeric(substr(x.data, 2 + 20 + 4, 2), false),
        bytea2numeric(substr(x.data, 2 + 20 + 4 + 2, 12), false)::double precision
    )::loopring.deposit_struct as deposit,
    (
        substr(x.data, 2 + 1, 20),
        bytea2numeric(substr(x.data, 2 + 1 + 20, 4), false),
        bytea2numeric(substr(x.data, 2 + 1 + 20 + 4, 2), false),
        bytea2numeric(substr(x.data, 2 + 1 + 20 + 4 + 2, 12), false)::double precision,
        bytea2numeric(substr(x.data, 2 + 1 + 20 + 4 + 2 + 12, 2), false),
        loopring.fn_decode_float_16(substr(x.data, 2 + 1 + 20 + 4 + 2 + 12 + 2, 2))::double precision
    )::loopring.withdraw_struct as withdraw,
    (
        bytea2numeric(substr(x.data, 2 + 1 + 4 + 4, 2), false),
        loopring.fn_decode_float_24(substr(x.data, 2 + 1 + 4 + 4 + 2, 3))::double precision,
        bytea2numeric(substr(x.data, 2 + 1 + 4 + 4 + 2 + 3, 2), false),
        loopring.fn_decode_float_16(substr(x.data, 2 + 1 + 4 + 4 + 2 + 3 + 2, 2))::double precision,
        bytea2numeric(substr(x.data, 2 + 1, 4), false),
        bytea2numeric(substr(x.data, 2 + 1 + 4, 4), false),
        substr(x.data, 2 + 1 + 4 + 4 + 2 + 3 + 2 + 2 + 4, 20),
        substr(x.data, 2 + 1 + 4 + 4 + 2 + 3 + 2 + 2 + 4 + 20, 20)
    )::loopring.transfer_struct as transfer,
    (
        bytea2numeric(substr(x.data, 2 + 4 + 4, 4), false),
        bytea2numeric(substr(x.data, 2 + 4 + 4 + 4, 4), false),
        bytea2numeric(substr(x.data, 2 + 4 + 4 + 4 + 4, 2), false),
        bytea2numeric(substr(x.data, 2 + 4 + 4 + 4 + 4 + 2, 2), false),
        loopring.fn_decode_float_24(substr(x.data, 2 + 4 + 4 + 4 + 4 + 2 + 2, 3))::double precision,
        loopring.fn_decode_float_24(substr(x.data, 2 + 4 + 4 + 4 + 4 + 2 + 2 + 3, 3))::double precision
    )::loopring.spot_trade_struct as spot_trade,
    (
        substr(x.data, 2 + 1, 20),
        bytea2numeric(substr(x.data, 2 + 1 + 20, 4), false),
        bytea2numeric(substr(x.data, 2 + 1 + 20 + 4, 2), false),
        loopring.fn_decode_float_16(substr(x.data, 2 + 1 + 20 + 4 + 2, 2))::double precision,
        substr(x.data, 2 + 1 + 20 + 4 + 2 + 2, 32)
    )::loopring.account_update_struct as account_update,
    (
        substr(x.data, 2, 20),
        bytea2numeric(substr(x.data, 2 + 20, 4), false),
        bytea2numeric(substr(x.data, 2 + 20 + 4, 2), false),
        get_byte(substr(x.data, 2 + 20 + 4 + 2, 1), 0),
        bytea2numeric(substr(x.data, 2 + 20 + 4 + 2 + 1, 12), false)::double precision
    )::loopring.amm_update_struct as amm_update,
    (
        substr(x.data, 2, 20),
        bytea2numeric(substr(x.data, 2 + 20, 4), false)
    )::loopring.signature_verification_struct as signature_verification
FROM generate_series(0, blockSize - 1) i, LATERAL (
    SELECT (substr(_data, 99+i*29, 29) || substr(_data, 99+blockSize*29+i*39, 39)) AS data
) x;
END;
$$
LANGUAGE PLPGSQL
IMMUTABLE
STRICT
;