{% macro bungee_SocketBridge(blockchain) %}

select
    contract_address,
    evt_tx_hash,
    evt_index,
    evt_block_time,
    evt_block_number,
    amount,
    token,
    toChainId,
    bridgeName,
    sender,
    receiver,
    metadata,
    '{{ blockchain }}' as source_chain,
    {{ dbt_utils.generate_surrogate_key(['evt_tx_hash', 'evt_index']) }} as transfer_id
from {{ source('socket_v2_' ~ blockchain, 'SocketGateway_evt_SocketBridge') }}
{% if is_incremental() %}
where {{ incremental_predicate('evt_block_time') }}
{% endif %}

{% endmacro %}
