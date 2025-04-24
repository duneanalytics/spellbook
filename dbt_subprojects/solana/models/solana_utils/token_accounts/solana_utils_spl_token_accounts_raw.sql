{{
  config (
    schema='solana_utils'
    , alias='spl_token_accounts_raw'
    , partition_by=['block_date']
    , materialized='incremental'
    , file_format='delta'
    , incremental_strategy='append'
    , unique_key=['block_date', 'address', 'address_prefix', 'unique_instruction_key']
  )
}}

with init as (
    --Init v1: events contain mint address and owner address
    SELECT
        CAST(DATE_TRUNC('day', call_block_time) as DATE) AS block_date
        , call_block_time AS block_time
        , account_account AS address
        , SUBSTRING(account_account, 1, 2) AS address_prefix
        , 'init' AS event_type
        , account_owner as token_balance_owner
        , account_mint as token_mint_address
        , CONCAT(
            lpad(cast(call_block_slot as varchar), 12, '0'), '-',
            lpad(cast(call_tx_index as varchar), 6, '0'), '-',
            lpad(cast(coalesce(call_outer_instruction_index, 0) as varchar), 4, '0'), '-',
            lpad(cast(coalesce(call_inner_instruction_index, 0) as varchar), 4, '0')
        ) AS unique_instruction_key
        , call_block_slot as block_slot
        , call_tx_index as tx_index
        , coalesce(call_outer_instruction_index, 0) as outer_instruction_index
        , coalesce(call_inner_instruction_index, 0) as inner_instruction_index
    FROM
        {{ source('spl_token_solana', 'spl_token_call_initializeaccount') }}
    WHERE
        1=1
        {% if is_incremental() or true %}
        AND {{ incremental_predicate('call_block_time') }}
        {% endif %}

    UNION ALL

    --Init v2
    SELECT
        CAST(DATE_TRUNC('day', call_block_time) as DATE) AS block_date
        , call_block_time AS block_time
        , account_account AS address
        , SUBSTRING(account_account, 1, 2) AS address_prefix
        , 'init' AS event_type
        , owner as token_balance_owner
        , account_mint as token_mint_address
        , CONCAT(
            lpad(cast(call_block_slot as varchar), 12, '0'), '-',
            lpad(cast(call_tx_index as varchar), 6, '0'), '-',
            lpad(cast(coalesce(call_outer_instruction_index, 0) as varchar), 4, '0'), '-',
            lpad(cast(coalesce(call_inner_instruction_index, 0) as varchar), 4, '0')
        ) AS unique_instruction_key
        , call_block_slot as block_slot
        , call_tx_index as tx_index
        , coalesce(call_outer_instruction_index, 0) as outer_instruction_index
        , coalesce(call_inner_instruction_index, 0) as inner_instruction_index
    FROM {{ source('spl_token_solana', 'spl_token_call_initializeaccount2') }}
    WHERE
        1=1
        {% if is_incremental() or true %}
        AND {{ incremental_predicate('call_block_time') }}
        {% endif %}

    UNION ALL

    --Init v3
    SELECT
        CAST(DATE_TRUNC('day', call_block_time) as DATE) AS block_date
        , call_block_time AS block_time
        , account_account AS address
        , SUBSTRING(account_account, 1, 2) AS address_prefix
        , 'init' AS event_type
        , owner as token_balance_owner
        , account_mint as token_mint_address
        , CONCAT(
            lpad(cast(call_block_slot as varchar), 12, '0'), '-',
            lpad(cast(call_tx_index as varchar), 6, '0'), '-',
            lpad(cast(coalesce(call_outer_instruction_index, 0) as varchar), 4, '0'), '-',
            lpad(cast(coalesce(call_inner_instruction_index, 0) as varchar), 4, '0')
        ) AS unique_instruction_key
        , call_block_slot as block_slot
        , call_tx_index as tx_index
        , coalesce(call_outer_instruction_index, 0) as outer_instruction_index
        , coalesce(call_inner_instruction_index, 0) as inner_instruction_index
    FROM {{ source('spl_token_solana', 'spl_token_call_initializeaccount3') }}
    WHERE 
        1=1
        {% if is_incremental() or true %}
        AND {{ incremental_predicate('call_block_time') }}
        {% endif %}
)
, owner_change as (
    -- Owner Changes: Only owner changes, mint persists, mint address not in data
    SELECT
        CAST(DATE_TRUNC('day', call_block_time) as DATE) AS block_date
        , call_block_time AS block_time
        , account_owned AS address
        , SUBSTRING(account_owned, 1, 2) AS address_prefix
        , 'owner_change' AS event_type
        , newAuthority as token_balance_owner
        , null as token_mint_address
        , CONCAT(
            lpad(cast(call_block_slot as varchar), 12, '0'), '-',
            lpad(cast(call_tx_index as varchar), 6, '0'), '-',
            lpad(cast(coalesce(call_outer_instruction_index, 0) as varchar), 4, '0'), '-',
            lpad(cast(coalesce(call_inner_instruction_index, 0) as varchar), 4, '0')
        ) AS unique_instruction_key
        , call_block_slot as block_slot
        , call_tx_index as tx_index
        , coalesce(call_outer_instruction_index, 0) as outer_instruction_index
        , coalesce(call_inner_instruction_index, 0) as inner_instruction_index
    FROM {{ source('spl_token_solana', 'spl_token_call_setauthority') }}
    WHERE
        json_extract_scalar(authorityType, '$.AuthorityType') = 'AccountOwner'
        {% if is_incremental() or true %}
        AND {{ incremental_predicate('call_block_time') }}
        {% endif %}
)
, close as (
    --Closure events only contain the token account address
    SELECT
        CAST(DATE_TRUNC('day', call_block_time) as DATE) AS block_date
        , call_block_time AS block_time
        , account_account AS address
        , SUBSTRING(account_account, 1, 2) AS address_prefix
        , 'close' AS event_type
        , null as token_balance_owner
        , null as token_mint_address
        , CONCAT(
            lpad(cast(call_block_slot as varchar), 12, '0'), '-',
            lpad(cast(call_tx_index as varchar), 6, '0'), '-',
            lpad(cast(coalesce(call_outer_instruction_index, 0) as varchar), 4, '0'), '-',
            lpad(cast(coalesce(call_inner_instruction_index, 0) as varchar), 4, '0')
        ) AS unique_instruction_key
        , call_block_slot as block_slot
        , call_tx_index as tx_index
        , coalesce(call_outer_instruction_index, 0) as outer_instruction_index
        , coalesce(call_inner_instruction_index, 0) as inner_instruction_index
    FROM {{ source('spl_token_solana', 'spl_token_call_closeaccount') }}
    WHERE
        1=1
        {% if is_incremental() or true %}
        AND {{ incremental_predicate('call_block_time') }}
        {% endif %}
)
, raw_events as (
    select * from init
    union all
    select * from owner_change
    union all
    select * from close
)
, final as (
    select
        raw.*
    from
        raw_events as raw
    {% if is_incremental() -%}
    left join {{ this }} as existing
        on raw.block_date = existing.block_date
        and raw.address = existing.address
        and raw.address_prefix = existing.address_prefix
        and raw.unique_instruction_key = existing.unique_instruction_key
        and {{ incremental_predicate('existing.block_time') }}
    where
        existing.address is null
    {%- endif %}
)
select * from final