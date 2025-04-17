{{
  config(
	schema='solana_utils'
    , alias='token_accounts_scd2'
    , materialized='incremental'
    , file_format='delta'
    , incremental_strategy='merge'
	, partition_by=['token_account_prefix']
	, unique_key=['token_account_prefix', 'token_account', 'valid_from_unique_instruction_key']
  )
}}

-- Step 1: Source data with forward-filled account_mint and SCD logic
with ranked_src as (
	select
		token_account_prefix
		, token_account
		, event_type
		, account_owner
		, account_mint -- Keep original mint for later logic
		, max(
			case
			when account_mint is not null then account_mint
			end
			) over (
			partition by token_account_prefix, token_account
			order by unique_instruction_key asc
			rows between unbounded preceding and current row
		) as last_non_null_account_mint -- get the latest non-null account_mint up to this point
		, unique_instruction_key as valid_from_unique_instruction_key
		, lead(unique_instruction_key) over (
				partition by token_account_prefix, token_account
				order by unique_instruction_key asc
			) as valid_to_unique_instruction_key
		, row_number() over (partition by token_account_prefix, token_account order by unique_instruction_key desc) as is_current
	from
		{{ ref('solana_utils_token_accounts_raw') }}
	{% if is_incremental() -%}
	where
		{{ incremental_predicate('block_time') }}
	{%- endif %}
)
, src as (
  select
    token_account_prefix
    , token_account
    , event_type
    , account_owner as token_balance_owner
    , CASE
        WHEN event_type = 'owner_change' THEN last_non_null_account_mint
        ELSE account_mint
    END AS token_mint_address
    , valid_from_unique_instruction_key
    , coalesce(valid_to_unique_instruction_key, '999999999-999999-9999-9999') as valid_to_unique_instruction_key
    , if(is_current = 1, 1, 0) as is_current
  from
    ranked_src
)
{% if is_incremental() -%}
-- Step 2: Active rows from current table
, dst as (
	select
		token_account_prefix
		, token_account
		, event_type
		, token_balance_owner
		, token_mint_address
		, valid_from_unique_instruction_key
		, valid_to_unique_instruction_key
		, is_current
	from {{ this }}
	where is_current = 1
)
-- Step 3: Find earliest new version per token_account in source
, earliest_src_per_token_account as (
	select
		token_account
		, token_account_prefix
		, min(valid_from_unique_instruction_key) as new_valid_from_unique_instruction_key
	from src
	group by
		token_account
		, token_account_prefix
)
-- Step 4: Expire target rows with newer source rows
, to_expire as (
	select
    	d.token_account
		, d.token_account_prefix
		, d.valid_from_unique_instruction_key
		, e.new_valid_from_unique_instruction_key as new_valid_to_unique_instruction_key
	from dst as d
	join earliest_src_per_token_account as e
		on d.token_account = e.token_account
		and d.token_account_prefix = e.token_account_prefix
		and d.valid_from_unique_instruction_key < e.new_valid_from_unique_instruction_key
)
-- Step 5: Update valid_to and is_current flags
, updated_dst as (
	select
		d.token_account_prefix
		, d.token_account
		, d.event_type
		, d.token_balance_owner
		, d.token_mint_address
		, d.valid_from_unique_instruction_key
		, coalesce(e.new_valid_to_unique_instruction_key, d.valid_to_unique_instruction_key) as valid_to_unique_instruction_key
		, case
			when e.new_valid_to_unique_instruction_key is not null then 0
			else d.is_current
		end as is_current
	from dst as d
	left join to_expire as e
		on d.token_account = e.token_account
		and d.token_account_prefix = e.token_account_prefix
		and d.valid_from_unique_instruction_key = e.valid_from_unique_instruction_key
)
-- Step 6: Filter out overlapping src rows already handled by updated_dst
, final_src as (
	select
    	s.*
	from src as s
	left join updated_dst as d
		on s.token_account = d.token_account
		and s.token_account_prefix = d.token_account_prefix
		and s.valid_from_unique_instruction_key = d.valid_from_unique_instruction_key
  	where
    	d.token_account is null
)
-- Step 7: Merge-ready output
select
	*
from
	updated_dst
union all
select
	*
from
	final_src
{% else -%}
-- Full refresh: just emit all source rows with SCD logic applied
select * from src
{%- endif %}