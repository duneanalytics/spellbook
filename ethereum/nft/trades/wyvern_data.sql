CREATE SCHEMA IF NOT EXISTS nft;

DROP TABLE nft.wyvern_data;
CREATE TABLE IF NOT EXISTS nft.wyvern_data(
    call_tx_hash bytea,
    trade_type text,
    erc_standard text,
    exchange_contract_address bytea,
    nft_contract_address bytea,
    currency_token bytea,
    original_amount numeric,
    buyer bytea,
    buyer_when_aggr bytea,
    seller bytea,
    token_id text,
    call_trace_address varchar,
    original_currency_address bytea[],
    fees numeric,
    PRIMARY KEY (call_tx_hash, seller)
);

CREATE INDEX IF NOT EXISTS nft_wyv_data_tx_hash_idx ON nft.wyvern_data (call_tx_hash);
CREATE INDEX IF NOT EXISTS nft_wyv_data_currency_token_idx ON nft.wyvern_data (currency_token);
CREATE INDEX IF NOT EXISTS nft_wyv_data_nft_contract_addr_idx ON nft.wyvern_data (nft_contract_address);
CREATE INDEX IF NOT EXISTS nft_wyv_data_token_id_idx ON nft.wyvern_data (token_id);

CREATE INDEX IF NOT EXISTS nft_wyv_data_tx_hash_token_id_idx ON nft.wyvern_data (call_tx_hash, token_id);
CREATE INDEX IF NOT EXISTS nft_wyv_data_tx_hash_trace_addr_idx ON nft.wyvern_data (call_tx_hash, call_trace_address);
CREATE INDEX IF NOT EXISTS nft_wyv_data_tx_hash_nft_contract_addr_idx ON nft.wyvern_data (call_tx_hash, nft_contract_address);

