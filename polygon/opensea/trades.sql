CREATE SCHEMA IF NOT EXISTS opensea
;

CREATE TABLE IF NOT EXISTS opensea.trades (
    block_time timestamptz
   ,nft_contract_address bytea
   ,nft_token_id text
   ,erc_standard text
   ,platform text
   ,platform_version text
   ,trade_type text
   ,number_of_items numeric
   ,seller bytea
   ,buyer bytea
   ,currency_contract bytea
   ,original_currency text
   ,original_amount numeric
   ,original_amount_raw numeric
   ,usd_amount numeric
   ,fee_receive_address bytea
   ,fee_amount numeric
   ,fee_amount_raw numeric
   ,fee_usd_amount numeric
   ,royalty_receive_address bytea
   ,royalty_amount numeric
   ,royalty_amount_raw numeric
   ,royalty_usd_amount numeric
   ,exchange_contract_address bytea
   ,block_number int4
   ,tx_hash bytea
   ,tx_from bytea
   ,tx_to bytea
   ,trade_id int4
);

CREATE UNIQUE INDEX IF NOT EXISTS opensea_trades_tx_hash_trade_id_idx ON opensea.trades (tx_hash, trade_id);
CREATE INDEX IF NOT EXISTS opensea_trades_block_time_idx ON opensea.trades (block_time);
CREATE INDEX IF NOT EXISTS opensea_trades_seller_idx ON opensea.trades (seller);
CREATE INDEX IF NOT EXISTS opensea_trades_buyer_idx ON opensea.trades (buyer);
CREATE INDEX IF NOT EXISTS opensea_trades_fee_receive_address_idx ON opensea.trades (fee_receive_address);
CREATE INDEX IF NOT EXISTS opensea_trades_royalty_receive_address_idx ON opensea.trades (royalty_receive_address);
CREATE INDEX IF NOT EXISTS opensea_trades_nft_contract_address_nft_token_id_block_time_idx ON opensea.trades (nft_contract_address, nft_token_id, block_time);

