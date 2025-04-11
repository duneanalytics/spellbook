{{
  config(
    schema='solana_utils'
    , alias='token_accounts'
    , partition_by=['token_account_prefix']
    , materialized='incremental'
    , file_format='delta'
    , incremental_strategy='merge'
	, unique_key=['token_account_prefix', 'token_account', 'unique_instruction_key']
  )
}}

-- Step 1: Source data with forward-filled account_mint and SCD logic
with ranked_src as (
	select
		token_account_prefix
		, token_account
		, event_type
		, account_owner
		, coalesce(
			account_mint
			, last_value(account_mint ignore nulls) over (
				partition by token_account
				order by unique_instruction_key
				rows between unbounded preceding and current row
			)
		) as account_mint
		, unique_instruction_key
		, block_time as valid_from
		, lead(block_time) over (
			partition by token_account
			order by unique_instruction_key
		) as next_valid_from
		, row_number() over (
			partition by token_account
			order by unique_instruction_key desc
		) as row_num
		, md5(token_account || unique_instruction_key) as scd_id
	from {{ ref('solana_utils_token_accounts_raw') }}
	{%- if is_incremental() -%}
	where {{ incremental_predicate('block_time') }}
	{%- endif -%}
)
, src as (
	select
		token_account_prefix
		, token_account
		, event_type
		, account_owner
		, account_mint
		, unique_instruction_key
		, valid_from
		, coalesce(next_valid_from, TIMESTAMP '9999-12-31 23:59:59') as valid_to
		, row_num = 1 as is_current
		, scd_id
	from ranked_src
)
{%- if is_incremental() -%}
-- Step 2: Active rows from current table
, dst as (
	select
		token_account
		, token_account_prefix
		, unique_instruction_key
		, valid_from
		, valid_to
		, is_current
		, event_type
		, account_owner
		, account_mint
		, md5(token_account || unique_instruction_key) as scd_id
	from {{ this }}
    where is_current = true -- consider building this as model, if slow, and read here instead
)
-- Step 3: Find earliest new version per token_account in source
, earliest_src_per_token_account as (
	select
		token_account
        , token_account_prefix
		, min(valid_from) as new_valid_from
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
		, d.unique_instruction_key
		, e.new_valid_from as new_valid_to
	from dst d
	join earliest_src_per_token_account e
		on d.token_account = e.token_account
        and d.token_account_prefix = e.token_account_prefix
		and e.new_valid_from > d.valid_from
)
-- Step 5: Update `valid_to` and `is_current` flags
, updated_dst as (
	select
		d.token_account_prefix
		, d.token_account
		, d.event_type
		, d.account_owner
		, d.account_mint
		, d.unique_instruction_key
		, d.valid_from
		, coalesce(e.new_valid_to, d.valid_to) as valid_to
		, case when e.new_valid_from is not null then false else d.is_current end as is_current
		, d.scd_id
	from dst d
	left join to_expire e
		on d.token_account = e.token_account
        and d.token_account_prefix = e.token_account_prefix
		and d.unique_instruction_key = e.unique_instruction_key
)
-- Step 6: Filter out overlapping src rows already handled by updated_dst
, final_src as (
	select *
	from src
	where scd_id not in (
		select scd_id from updated_dst
	)
)
-- Step 7: Merge-ready output
select * from updated_dst
union all
select * from final_src
{%- else -%}
-- Full refresh: just emit all source rows with SCD logic applied
select * from src
{%- endif -%}