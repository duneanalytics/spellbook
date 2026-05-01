{{ config(
    schema = 'paintswap_fantom',
    
    alias = 'base_trades',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_number','tx_hash','sub_tx_trade_id'],
    )
}}

{% set project_start_date = "2022-07-16" %}

SELECT 'fantom' as blockchain
, 'paintswap' as project
, 'v1' as project_version
, ps.evt_block_time AS block_time
, ps.evt_block_number AS block_number
, ps.nfts[1] AS nft_contract_address
, ps.tokenIds[1] AS nft_token_id
, ps.amount AS nft_amount
, ps.seller AS seller
, ps.buyer AS buyer
, CASE WHEN ps.offerId = 0 THEN 'Buy' ELSE 'Sell' END AS trade_category
, 'secondary' AS trade_type
, ps.price AS price_raw
, 0x0000000000000000000000000000000000000000 AS currency_contract
, ps.contract_address AS project_contract_address
, ps.evt_tx_hash AS tx_hash
, ps.evt_index AS sub_tx_trade_id
, SUM(CASE WHEN traces.to = 0x045ef160107ed663d10c5a31c7d2ec5527eea1d0 THEN traces.value END) AS platform_fee_amount_raw
, 0x045ef160107ed663d10c5a31c7d2ec5527eea1d0 AS platform_fee_address
, SUM(CASE WHEN traces.to != 0x045ef160107ed663d10c5a31c7d2ec5527eea1d0 THEN traces.value END) AS royalty_fee_amount_raw
, MAX(CASE WHEN traces.to != 0x045ef160107ed663d10c5a31c7d2ec5527eea1d0 THEN traces.to END) AS royalty_fee_address
FROM {{ source('paintswap_fantom','PaintSwapMarketplaceV3_evt_Sold') }} ps
LEFT JOIN {{ source('fantom','traces') }} traces ON traces.block_number=ps.evt_block_number
    AND traces.block_time >= TIMESTAMP '{{project_start_date}}'
    AND traces.tx_hash=ps.evt_tx_hash
    AND traces."from"=0x3e72029f9050ee7d8dc1eecf533d3fbe17ec0a47
    AND traces.to <> 0x31f63a33141ffee63d4b26755430a390acdd8a4d
{% if is_incremental() %}
WHERE {{incremental_predicate('ps.evt_block_time')}}
{% else %}
WHERE ps.evt_block_time >= TIMESTAMP '{{project_start_date}}'
{% endif %}
GROUP BY 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 19