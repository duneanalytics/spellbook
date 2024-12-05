{% macro lifi_extract_bridge_data(blockchain) %}

{% set bridge_data_fields = [
    'transactionId',
    'bridge',
    'integrator',
    'referrer',
    'sendingAssetId',
    'receiver',
    'minAmount',
    'destinationChainId'
] %}

select
    contract_address,
    evt_tx_hash as tx_hash,
    evt_index,
    evt_block_time as block_time,
    evt_block_number as block_number,
    cast(date_trunc('day', evt_block_time) as date) as block_date,
    {% for field in bridge_data_fields %}
    {% if field in ['transactionId', 'referrer', 'sendingAssetId', 'receiver'] %}
    from_hex(json_extract_scalar(bridgedata, '$.{{ field }}')) as {{ field }},
    {% elif field == 'minAmount' %}
    cast(json_extract_scalar(bridgedata, '$.{{ field }}') as double) as {{ field }},
    {% else %}
    json_extract_scalar(bridgedata, '$.{{ field }}') as {{ field }},
    {% endif %}
    {% endfor %}
    '{{ blockchain }}' as source_chain,
    {{ dbt_utils.generate_surrogate_key(['evt_tx_hash', 'evt_index']) }} as transfer_id
from {{ source('lifi_' ~ blockchain, 'LiFiDiamond_v2_evt_LiFiTransferStarted') }}
{% if is_incremental() %}
where {{ incremental_predicate('evt_block_time') }}
{% endif %}

{% endmacro %}
