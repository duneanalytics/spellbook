{#-
  POC / throwaway (CUR2-2963). CDF-aware fork of transfers_enrich for the delta_cdf
  strategy. Differences vs transfers_enrich:
    - base CTE is the CDF change feed (source_changes), which internally bootstraps a
      full snapshot on first build / --full-refresh and otherwise reads table_changes
      strictly after the stored watermark.
    - the prices window is bounded by the CHANGE SET's block_time range (not the
      wall-clock incremental lookback), which is the whole point of CDF.
    - _change_type / _commit_version are carried to the final projection ONLY on the
      incremental path, so the strategy macro can capture max(_commit_version). On
      bootstrap they are dropped so the CTAS target schema stays clean and matches the
      baseline.
  Delete with the _poc_cdf models when the A/B is done.
-#}
{% macro transfers_enrich_cdf(
	base_relation = null
	, blockchain = null
	, tokens_erc20_model = source('tokens', 'erc20')
	, prices_interval = 'hour'
	, trusted_tokens_model = source('prices', 'trusted_tokens')
	, usd_amount_threshold = 1000000000
	)
%}

{%- if blockchain is none or blockchain == '' -%}
	{{ exceptions.raise_compiler_error("blockchain parameter cannot be null or empty") }}
{%- endif -%}
{%- if base_relation is none or base_relation == '' -%}
	{{ exceptions.raise_compiler_error("base_relation parameter cannot be null or empty") }}
{%- endif -%}

with base_transfers as (
	{{ source_changes(base_relation) }}
)
, prices as (
	select
		timestamp
		, blockchain
		, contract_address
		, decimals
		, symbol
		, price
	from
		{{ source('prices_external', prices_interval) }}
	where
		timestamp >= (select date_trunc('{{ prices_interval }}', min(block_time)) from base_transfers)
		and timestamp <= (select max(block_time) from base_transfers)
)
, trusted_tokens as (
	select
		blockchain
		, contract_address
	from
		{{ trusted_tokens_model }}
)
, transfers as (
	select
		t.unique_key
		, t.blockchain
		, t.block_month
		, t.block_date
		, t.block_time
		, t.block_number
		, t.tx_hash
		, t.evt_index
		, t.trace_address
		, t.token_standard
		, t.tx_from
		, t.tx_to
		, t.tx_index
		, t."from"
		, t.to
		, t.contract_address
		, coalesce(tokens_erc20.symbol, prices.symbol) as symbol
		, t.amount_raw
		, t.amount_raw / power(10, coalesce(tokens_erc20.decimals, prices.decimals)) as amount
		, prices.price as price_usd
		, t.amount_raw / power(10, coalesce(tokens_erc20.decimals, prices.decimals)) * prices.price as amount_usd
		, case when trusted_tokens.blockchain is not null then true else false end as is_trusted_token
		, t._updated_at
		{%- if is_incremental() %}
		, t._change_type
		, t._commit_version
		{%- endif %}
	from
		base_transfers as t
	left join {{ tokens_erc20_model }} as tokens_erc20
		on tokens_erc20.blockchain = t.blockchain
		and tokens_erc20.contract_address = t.contract_address
	left join trusted_tokens
		on trusted_tokens.blockchain = t.blockchain
		and trusted_tokens.contract_address = t.contract_address
	left join prices
		on date_trunc('{{ prices_interval }}', t.block_time) = prices.timestamp
		and t.blockchain = prices.blockchain
		and t.contract_address = prices.contract_address
)
, final as (
	select
		unique_key
		, blockchain
		, block_month
		, block_date
		, block_time
		, block_number
		, tx_hash
		, evt_index
		, trace_address
		, token_standard
		, tx_from
		, tx_to
		, tx_index
		, "from"
		, to
		, contract_address
		, symbol
		, amount_raw
		, amount
		, price_usd
		, case
			when is_trusted_token = true then amount_usd
			when (is_trusted_token = false and amount_usd < {{ usd_amount_threshold }}) then amount_usd
			when (is_trusted_token = false and amount_usd >= {{ usd_amount_threshold }}) then cast(null as double)
			end as amount_usd
		, _updated_at
		{%- if is_incremental() %}
		, _change_type
		, _commit_version
		{%- endif %}
	from
		transfers
)
select
	*
from
	final
{%- endmacro %}
