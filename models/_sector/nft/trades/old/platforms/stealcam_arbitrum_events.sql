{{ config(
    schema = 'stealcam_arbitrum',
    alias = 'events',
    
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['unique_trade_id']
    )
}}


{% set project_start_date = "TIMESTAMP '2023-03-10'" %}

with stealcam as (
select
    *
    ,case when value > uint256 '0' then cast((value-(0.001*pow(10,18)))/11.0+(0.001*pow(10,18)) as uint256) else uint256 '0' end as surplus_value
FROM {{ source('stealcam_arbitrum', 'Stealcam_evt_Stolen') }} sc
{% if is_incremental() %}
WHERE evt_block_time >= date_trunc('day', now() - interval '7' day)
{% endif %}
{% if not is_incremental() %}
WHERE evt_block_time >= {{project_start_date}}
{% endif %}

)

SELECT 'arbitrum' AS blockchain
, 'stealcam' AS project
, 'v1' AS version
, sc.evt_block_time AS block_time
, sc.evt_block_number AS block_number
, 'Single Item Trade' AS trade_type
, 'Buy' AS trade_category
, CASE WHEN sc.value=uint256 '0' THEN 'Mint' ELSE 'Trade' END AS evt_type
, sc."from" AS seller
, sc.to AS buyer
, sc.contract_address AS nft_contract_address
, 'Stealcam' AS collection
, sc.id AS token_id
, 'erc721' AS token_standard
, uint256 '1' AS number_of_items
, 0x82af49447d8a07e3bd95bd0d56f35241523fbab1 AS currency_contract
, 'ETH' AS currency_symbol
, sc.value AS amount_raw
, CAST(sc.value/POWER(10, 18) AS DOUBLE) AS amount_original
, CAST(pu.price*sc.value/POWER(10, 18) AS DOUBLE) AS amount_usd
, sc.contract_address AS project_contract_address
, CAST(NULL AS varchar) AS aggregator_name
, CAST(NULL AS varbinary) AS aggregator_address
, sc.evt_tx_hash AS tx_hash
, at."from" AS tx_from
, at.to AS tx_to
, CAST(0.1*surplus_value AS uint256) AS platform_fee_amount_raw
, CAST(0.1*surplus_value/POWER(10, 18) AS double) AS platform_fee_amount
, CAST(pu.price*0.1*surplus_value/POWER(10, 18) AS double) AS platform_fee_amount_usd
, CAST(coalesce(100*(0.1*surplus_value/sc.value),0) AS double) AS platform_fee_percentage
, 'ETH' as royalty_fee_currency_symbol
, CAST(0.45*surplus_value AS uint256) AS royalty_fee_amount_raw
, CAST(0.45*surplus_value/POWER(10, 18) AS double) AS royalty_fee_amount
, CAST(pu.price*0.45*surplus_value/POWER(10, 18) AS double) AS royalty_fee_amount_usd
, CAST(coalesce(100*(0.45*surplus_value/sc.value),0) AS double) AS royalty_fee_percentage
, m._creator AS royalty_fee_receive_address
, sc.evt_index
, 'arbitrum-stealcam-' || cast(sc.evt_tx_hash as varchar)|| '-' || cast(sc.evt_index as varchar) AS unique_trade_id
FROM stealcam sc
INNER JOIN {{ source('arbitrum', 'transactions') }} at ON at.block_number=sc.evt_block_number
    AND at.hash=sc.evt_tx_hash
    {% if is_incremental() %}
    AND at.block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}
    {% if not is_incremental() %}
    AND at.block_time >= {{project_start_date}}
    {% endif %}
LEFT JOIN {{ ref('prices_usd_forward_fill') }} pu ON pu.blockchain = 'ethereum'
    AND pu.contract_address=0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2
    AND pu.minute=date_trunc('minute', sc.evt_block_time)
    {% if is_incremental() %}
    AND pu.minute >= date_trunc('day', now() - interval '7' day)
    {% endif %}
    {% if not is_incremental() %}
    AND pu.minute >= {{project_start_date}}
    {% endif %}
INNER JOIN {{ source('stealcam_arbitrum', 'Stealcam_call_mint') }} m ON m.call_success
    AND m.id=sc.id
    {% if is_incremental() %}
    AND m.call_block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}
    {% if not is_incremental() %}
    AND m.call_block_time >= {{project_start_date}}
    {% endif %}
