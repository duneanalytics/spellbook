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

select * from (
select
	t.blockchain
	, t.block_month
	, max(t._updated_at) as last_update_date
from
	{{ ref('tokens_transfers') }} as t
group by
	t.blockchain
	, t.block_month
)
{% if is_incremental -%}
	where {{incremental_predicate(last_update_date)}}
{% endif -%}
