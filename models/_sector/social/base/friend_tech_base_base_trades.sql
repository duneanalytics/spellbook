{{ config(
    schema = 'friend_tech_base',
    alias = 'base_trades',
    file_format = 'delta',
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = ['tx_hash','evt_index']
    )
}}

{% set friend_tech_start_date = '2023-08-10' %}

SELECT 
    'base' AS blockchain
    , evt_block_time AS block_time
    , evt_block_number AS block_number
    , 'friend_tech' AS project
    , trader
    , subject
    , CASE WHEN isBuy = TRUE THEN 'buy' ELSE 'sell' END AS trade_side
    , ethAmount/1e18 AS amount_original
    , shareAmount AS share_amount
    , subjectEthAmount/1e18 AS subject_fee_amount
    , protocolEthAmount/1e18 AS protocol_fee_amount
    , 0x0000000000000000000000000000000000000000 AS currency_contract
    , 'ETH' AS currency_symbol
    , supply
    , evt_tx_hash AS tx_hash
    , evt_index
    , contract_address
FROM {{source('friendtech_base', 'FriendtechSharesV1_evt_Trade')}}
{% if is_incremental() %}
WHERE {{ incremental_predicate('evt_block_time') }}
{% else %}
WHERE evt_block_time >= TIMESTAMP '{{friend_tech_start_date}}'
{% endif %}