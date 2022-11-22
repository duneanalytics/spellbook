CREATE TABLE IF NOT EXISTS seaport.transactions (
     block_time timestamptz  
    ,nft_contract_address bytea  
    ,nft_project_name text  
    ,nft_token_id text  
    ,erc_standard text  
    ,order_type text  
    ,purchase_method text  
    ,trade_type text  
    ,nft_transfer_count int8  
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
    ,erc721_transfer_count int8  
    ,erc1155_transfer_count int8  
    ,erc721_item_count int8  
    ,erc1155_item_count int8  
    ,exchange_contract_address bytea  
    ,zone_address bytea  
    ,platform text  
    ,block_number int4  
    ,tx_hash bytea  
    ,tx_from bytea  
    ,tx_to bytea  
    ,call_function text  
    ,order_type_id text  
    ,param1 text  
    ,param2 text  
    ,param3 text
);

CREATE UNIQUE INDEX IF NOT EXISTS seaport_transactions_tx_hash_idx ON seaport.transactions (tx_hash);
CREATE INDEX IF NOT EXISTS seaport_transactions_block_time_idx ON seaport.transactions (block_time);
CREATE INDEX IF NOT EXISTS seaport_transactions_call_function_block_time_idx ON seaport.transactions (call_function, block_time);
CREATE INDEX IF NOT EXISTS seaport_transactions_seller_idx ON seaport.transactions (seller);
CREATE INDEX IF NOT EXISTS seaport_transactions_buyer_idx ON seaport.transactions (buyer);
CREATE INDEX IF NOT EXISTS seaport_transactions_fee_receive_address_idx ON seaport.transactions (fee_receive_address);
CREATE INDEX IF NOT EXISTS seaport_transactions_royalty_receive_address_idx ON seaport.transactions (royalty_receive_address);
CREATE INDEX IF NOT EXISTS seaport_transactions_nft_contract_address_nft_token_id_block_time_idx ON seaport.transactions (nft_contract_address, nft_token_id, block_time);

