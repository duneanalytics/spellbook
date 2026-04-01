{{ config(
    schema = 'zora_v2_ethereum',
    
    alias = 'base_trades',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_number','tx_hash','sub_tx_trade_id'],
    )
}}

SELECT
      'ethereum' as blockchain
    , 'zora' as project
    , 'v2' as project_version
    , evt_block_time AS block_time
    , evt_block_number AS block_number
    , contract_address AS project_contract_address
    , evt_tx_hash AS tx_hash
    , tokenContract AS nft_contract_address
    , tokenId AS nft_token_id
    , uint256 '1' as nft_amount
    , 'Auction' AS trade_category
    , 'secondary'AS trade_type
    , winner AS buyer
    , tokenOwner AS seller
    , CAST(amount+curatorFee as uint256) AS price_raw
    , auctionCurrency AS currency_contract
    , uint256 '0' AS platform_fee_amount_raw
    , uint256 '0' AS royalty_fee_amount_raw
    , CAST(NULL as varbinary) AS platform_fee_address
    , CAST(NULL as varbinary) AS royalty_fee_address
    , evt_index as sub_tx_trade_id
FROM {{ source('zora_ethereum','AuctionHouse_evt_AuctionEnded') }}
{% if is_incremental() %}
WHERE {{incremental_predicate('evt_block_time')}}
{% endif %}
