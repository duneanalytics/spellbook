{{
    config(
        schema = 'opensea_base',
        alias = 'events'
    )
}}

SELECT *
FROM (
    SELECT   
        blockchain
        , project
        , version
        , block_time
        , token_id
        , collection
        , amount_usd
        , token_standard
        , trade_type
        , number_of_items
        , trade_category 
        , evt_type
        , seller
        , buyer
        , amount_original
        , amount_raw
        , currency_symbol
        , currency_contract
        , nft_contract_address
        , project_contract_address
        , aggregator_name
        , aggregator_address
        , tx_hash
        , block_number
        , tx_from
        , tx_to
        , platform_fee_amount_raw
        , platform_fee_amount
        , platform_fee_amount_usd
        , platform_fee_percentage
        , royalty_fee_amount_raw
        , royalty_fee_amount
        , royalty_fee_amount_usd
        , royalty_fee_percentage
        , royalty_fee_receive_address
        , currency_symbol as royalty_fee_currency_symbol
        , unique_trade_id
        , currency_decimals
        , platform_fee_receive_address
        , royalty_fee_receive_address_1
        , royalty_fee_receive_address_2
        , royalty_fee_receive_address_3
        , royalty_fee_receive_address_4
        , royalty_fee_receive_address_5
        , royalty_fee_amount_raw_1
        , royalty_fee_amount_raw_2
        , royalty_fee_amount_raw_3
        , royalty_fee_amount_raw_4
        , royalty_fee_amount_raw_5
        , evt_index
        , right_hash
        , zone_address
        , estimated_price
        , is_private
        , sub_idx
        , sub_type
        , fee_wallet_name
    FROM {{ ref('opensea_v4_base_events') }}
)
