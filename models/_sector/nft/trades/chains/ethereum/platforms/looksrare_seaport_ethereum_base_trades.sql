{{ config(
    schema = 'looksrare_seaport_ethereum',
    
    alias = 'base_trades',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_number','tx_hash','sub_tx_trade_id'],
    )
}}

{% set looksrare_seaport_start_date = "cast('2023-06-28' as timestamp)" %}

SELECT
 'ethereum' as blockchain
, 'looksrare' as project
, 'seaport' as project_version
, s.evt_block_time AS block_time
, s.evt_block_number AS block_number
, s.evt_tx_hash AS tx_hash
, s.offerer AS seller
, s.recipient AS buyer
, 'Buy' AS trade_category
, 'secondary' AS trade_type
, from_hex(json_extract_scalar(s.offer[1], '$.token')) AS nft_contract_address
, CAST(json_extract_scalar(s.offer[1], '$.identifier') AS UINT256) AS nft_token_id
, CAST(json_extract_scalar(s.offer[1], '$.amount') AS UINT256) AS nft_amount
, CAST((CAST(json_extract_scalar(s.consideration[1], '$.amount') AS double)+CAST(try(json_extract_scalar(s.consideration[2], '$.amount')) AS double)) AS UINT256) AS price_raw
, {{ var("ETH_ERC20_ADDRESS") }} AS currency_contract
, CAST(json_extract_scalar(s.consideration[2], '$.amount') AS UINT256) AS platform_fee_amount_raw
, from_hex(json_extract_scalar(s.consideration[2], '$.recipient')) AS platform_fee_address
, UINT256 '0' AS royalty_fee_amount_raw
, from_hex(NULL) AS royalty_fee_address
, s.contract_address AS project_contract_address
, s.evt_index as sub_tx_trade_id
FROM {{ source('seaport_ethereum','Seaport_evt_OrderFulfilled') }} s
WHERE s.contract_address = 0x00000000000000adc04c56bf30ac9d3c0aaf14dc
AND s.zone = 0x0000000000000000000000000000000000000000
AND try(from_hex(json_extract_scalar(s.consideration[2], '$.recipient')))  = 0x1838de7d4e4e42c8eb7b204a91e28e9fad14f536
{% if is_incremental() %}
AND {{incremental_predicate('evt_block_time')}}
{% else %}
AND evt_block_time >= {{looksrare_seaport_start_date}}
{% endif %}
