CREATE TABLE IF NOT EXISTS seaport.transfers (
    block_time timestamptz
   ,nft_contract_address bytea
   ,nft_token_id text
   ,erc_standard text
   ,order_type text
   ,purchase_method text
   ,trade_type text
   ,nft_item_count int8
   ,seller bytea
   ,buyer bytea
   ,original_currency_contract bytea
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
   ,price_estimated boolean
   ,exchange_contract_address bytea
   ,zone_address bytea
   ,platform text
   ,block_number int4
   ,tx_hash bytea
   ,tx_from bytea
   ,tx_to bytea
   ,trade_id int4
   ,call_function text
   ,order_type_id text
   ,param1 text
   ,param2 text
   ,param3 text
);

CREATE UNIQUE INDEX IF NOT EXISTS seaport_transfers_tx_hash_trade_id_idx ON seaport.transfers (tx_hash, trade_id);
CREATE INDEX IF NOT EXISTS seaport_transfers_block_time_idx ON seaport.transfers (block_time);
CREATE INDEX IF NOT EXISTS seaport_transfers_call_function_block_time_idx ON seaport.transfers (call_function, block_time);
CREATE INDEX IF NOT EXISTS seaport_transfers_seller_idx ON seaport.transfers (seller);
CREATE INDEX IF NOT EXISTS seaport_transfers_buyer_idx ON seaport.transfers (buyer);
CREATE INDEX IF NOT EXISTS seaport_transfers_fee_receive_address_idx ON seaport.transfers (fee_receive_address);
CREATE INDEX IF NOT EXISTS seaport_transfers_royalty_receive_address_idx ON seaport.transfers (royalty_receive_address);
CREATE INDEX IF NOT EXISTS seaport_transfers_nft_contract_address_nft_token_id_block_time_idx ON seaport.transfers (nft_contract_address, nft_token_id, block_time);
