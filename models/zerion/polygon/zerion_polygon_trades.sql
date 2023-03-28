{{ config(
    alias = 'trades'
    ,partition_by = ['block_date']
    ,materialized = 'incremental'
    ,file_format = 'delta'
    ,incremental_strategy = 'merge'
    ,unique_key = ['block_date', 'blockchain', 'project', 'version', 'tx_hash', 'evt_index', 'trace_address']
    )
}}

{% set project_start_date = '2021-06-01' %}

SELECT 'polygon' AS blockchain
, swap.evt_block_time AS block_time
, swap.evt_block_number AS block_number
, swap.sender AS trader
, swap.outputToken AS token_sold_address
, swap.absoluteOutputAmount AS token_sold_amount
, swap.inputToken AS token_bought_address
, swap.absoluteInputAmount AS token_bought_amount
, pt.from AS tx_from
, pt.to AS tx_to
, swap.evt_tx_hash AS tx_hash
, swap.contract_address
, swap.evt_index
, swap.marketplaceFeeAmount AS marketplace_fee
, swap.protocolFeeAmount AS protocol_fee
FROM {{ source('zerion_polygon', 'Router_evt_Executed') }} swap
INNER JOIN {{ source('polygon','transactions') }} pt ON pt.block_number=swap.evt_block_number
    AND pt.hash=swap.evt_tx_hash
{% if not is_incremental() %}
WHERE swap.evt_block_time >= '{{project_start_date}}'
{% endif %}
{% if is_incremental() %}
WHERE swap.evt_block_time >= date_trunc("day", now() - interval '1 week')
{% endif %}