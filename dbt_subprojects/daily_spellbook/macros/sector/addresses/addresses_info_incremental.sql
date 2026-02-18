{% macro addresses_info_incremental(blockchain, staging_model) %}
-- When incremental: merge staging (incremental window only) with target. When not: staging is full history, no target yet.
select
	'{{ blockchain }}' as blockchain
	, nd.address
	, nd.address_prefix
	, nd.executed_tx_count + {% if is_incremental() %}coalesce(t.executed_tx_count, 0){% else %}0{% endif %} as executed_tx_count
	, {% if is_incremental() %}coalesce(nd.max_nonce, t.max_nonce){% else %}nd.max_nonce{% endif %} as max_nonce
	, {% if is_incremental() %}coalesce(nd.is_smart_contract, t.is_smart_contract){% else %}nd.is_smart_contract{% endif %} as is_smart_contract
	, {% if is_incremental() %}coalesce(nd.namespace, t.namespace){% else %}nd.namespace{% endif %} as namespace
	, {% if is_incremental() %}coalesce(nd.name, t.name){% else %}nd.name{% endif %} as name
	, {% if is_incremental() %}coalesce(t.first_funded_by, nd.first_funded_by){% else %}nd.first_funded_by{% endif %} as first_funded_by
	, {% if is_incremental() %}coalesce(t.first_funded_by_block_time, nd.first_funded_by_block_time){% else %}nd.first_funded_by_block_time{% endif %} as first_funded_by_block_time
	, coalesce(nd.tokens_received_count, 0) + {% if is_incremental() %}coalesce(t.tokens_received_count, 0){% else %}0{% endif %} as tokens_received_count
	, coalesce(nd.tokens_received_tx_count, 0) + {% if is_incremental() %}coalesce(t.tokens_received_tx_count, 0){% else %}0{% endif %} as tokens_received_tx_count
	, coalesce(nd.tokens_sent_count, 0) + {% if is_incremental() %}coalesce(t.tokens_sent_count, 0){% else %}0{% endif %} as tokens_sent_count
	, coalesce(nd.tokens_sent_tx_count, 0) + {% if is_incremental() %}coalesce(t.tokens_sent_tx_count, 0){% else %}0{% endif %} as tokens_sent_tx_count
	, {% if is_incremental() %}coalesce(t.first_transfer_block_time, nd.first_transfer_block_time){% else %}nd.first_transfer_block_time{% endif %} as first_transfer_block_time
	, {% if is_incremental() %}coalesce(nd.last_transfer_block_time, t.last_transfer_block_time){% else %}nd.last_transfer_block_time{% endif %} as last_transfer_block_time
	, {% if is_incremental() %}coalesce(t.first_received_block_number, nd.first_received_block_number){% else %}nd.first_received_block_number{% endif %} as first_received_block_number
	, {% if is_incremental() %}coalesce(nd.last_received_block_number, t.last_received_block_number){% else %}nd.last_received_block_number{% endif %} as last_received_block_number
	, {% if is_incremental() %}coalesce(t.first_sent_block_number, nd.first_sent_block_number){% else %}nd.first_sent_block_number{% endif %} as first_sent_block_number
	, {% if is_incremental() %}coalesce(nd.last_sent_block_number, t.last_sent_block_number){% else %}nd.last_sent_block_number{% endif %} as last_sent_block_number
	, coalesce(nd.received_volume_usd, 0) + {% if is_incremental() %}coalesce(t.received_volume_usd, 0){% else %}0{% endif %} as received_volume_usd
	, coalesce(nd.sent_volume_usd, 0) + {% if is_incremental() %}coalesce(t.sent_volume_usd, 0){% else %}0{% endif %} as sent_volume_usd
	, {% if is_incremental() %}coalesce(t.first_tx_block_time, nd.first_tx_block_time){% else %}nd.first_tx_block_time{% endif %} as first_tx_block_time
	, {% if is_incremental() %}coalesce(nd.last_tx_block_time, t.last_tx_block_time){% else %}nd.last_tx_block_time{% endif %} as last_tx_block_time
	, {% if is_incremental() %}coalesce(t.first_tx_block_number, nd.first_tx_block_number){% else %}nd.first_tx_block_number{% endif %} as first_tx_block_number
	, {% if is_incremental() %}coalesce(nd.last_tx_block_number, t.last_tx_block_number){% else %}nd.last_tx_block_number{% endif %} as last_tx_block_number
	, array_max(filter(array[nd.last_tx_block_time, nd.last_transfer_block_time{% if is_incremental() %}, t.last_seen{% endif %}], x -> x is not null)) as last_seen
	, array_max(filter(array[nd.last_tx_block_number, nd.last_received_block_number, nd.last_sent_block_number{% if is_incremental() %}, t.last_seen_block{% endif %}], x -> x is not null)) as last_seen_block
from
	{{ staging_model }} as nd
	{% if is_incremental() -%}
left join {{ this }} as t
	on t.address_prefix = nd.address_prefix
	and t.address = nd.address
where {{ incremental_predicate('nd.last_seen') }}
	{% endif -%}
{% endmacro %}
