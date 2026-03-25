{{ config(
        schema = 'tokens'
        , alias = 'transfers_last_updated'
        , materialized = 'incremental'
        , file_format = 'delta'
        , incremental_strategy = 'merge'
        , unique_key = ['blockchain', 'block_month']
        , merge_skip_unchanged = true
        )
}}

{% if is_incremental() -%}
with changed_keys as (
	select distinct
		t.blockchain
		, t.block_month
	from
		{{ ref('tokens_transfers') }} as t
	left join {{ this }} as d
		on t.blockchain = d.blockchain
		and t.block_month = d.block_month
	where
		t._updated_at > coalesce(d.last_update_date, timestamp '1900-01-01')
)
, recalc as (
	select
		t.blockchain
		, t.block_month
		, max(t._updated_at) as last_update_date
	from
		{{ ref('tokens_transfers') }} as t
	inner join changed_keys as k
		on t.blockchain = k.blockchain
		and t.block_month = k.block_month
	group by
		t.blockchain
		, t.block_month
)

select
	r.blockchain
	, r.block_month
	, r.last_update_date
from
	recalc as r
{% else -%}
select
	t.blockchain
	, t.block_month
	, max(t._updated_at) as last_update_date
from
	{{ ref('tokens_transfers') }} as t
group by
	t.blockchain
	, t.block_month
{% endif -%}
