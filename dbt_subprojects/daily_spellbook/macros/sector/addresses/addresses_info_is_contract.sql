{% macro addresses_info_is_contract(creation_traces, contracts) %}
select
	ct.address
	, varbinary_to_integer(varbinary_substring(ct.address, 1, 1)) as address_prefix
	, true as is_smart_contract
	, max(ct.block_time) as block_time
	, max_by(c.namespace, c.created_at) as namespace
	, max_by(c.name, c.created_at) as name
from
	{{ creation_traces }} as ct
left join {{ contracts }} as c
	on ct.address = c.address
{% if is_incremental() -%}
where {{ incremental_predicate('ct.block_time') }}
{% endif -%}
group by
	1
	, 2
{% endmacro %}
