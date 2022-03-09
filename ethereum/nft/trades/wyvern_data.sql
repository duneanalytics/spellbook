CREATE SCHEMA IF NOT EXISTS nft;

DROP TABLE nft.wyvern_data;
CREATE TABLE IF NOT EXISTS nft.wyvern_data(
        call_tx_hash bytea,
        trade_type text,
        erc_standard text,
        exchange_contract_address bytea,
        nft_contract_address bytea,
        nft_contract_address_when_aggr bytea,
        currency_token bytea,
        original_amount numeric,
        buyer bytea,
        buyer_when_aggr bytea,
        seller bytea,
        token_id text,
        call_trace_address varchar,
        original_currency_address bytea[],
        fees numeric
    PRIMARY KEY (call_tx_hash, seller)
);
