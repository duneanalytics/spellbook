{% set blockchain = 'ethereum' %}

{{ config(
    schema = 'bridge_' + blockchain,
    alias = 'flows',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['tx_hash','evt_index'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

SELECT bf.blockchain
, bf.project
, bf.project_version
, bf.flows_type
, bf.block_time
, bf.block_month
, bf.block_number
, bf.amount_raw
, bf.sender
, bf.recipient
, bf.local_token
, bf.remote_token
, bf.extra_data
, bf.tx_hash
, bf.evt_index
, bf.contract_address
FROM {{ref('bridge_ethereum_base_raw_flows')}} bf
{% if is_incremental() %}
LEFT JOIN {{this}} t ON t.tx_hash=bf.tx_hash AND t.evt_index=bf.evt_index
    AND t.blockchain IS NULL
WHERE {{incremental_predicate('bf.block_time')}}
{% endif %}