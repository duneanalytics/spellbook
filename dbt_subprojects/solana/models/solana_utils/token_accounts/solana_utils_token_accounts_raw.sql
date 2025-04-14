{{
  config (
    schema='solana_utils'
    , alias='token_account_raw'
    , partition_by=['token_account_prefix']
    , materialized='incremental'
    , file_format='delta'
    , incremental_strategy='delete+insert'
    , unique_key=['token_account', 'token_account_prefix', 'unique_instruction_key']
    , pre_hook='{{ enforce_join_distribution("PARTITIONED") }}'
  )
}}

with init as (
    --Init v1 -- events contain mint address, and owner address
    SELECT
        CAST(DATE_TRUNC('day', call_block_time) as DATE) AS block_date
        , call_block_time AS block_time
        , account_account AS token_account
        , SUBSTRING(account_account, 1, 2) AS token_account_prefix
        , 'init' AS event_type
        , account_owner
        , account_mint
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
        {% if is_incremental() %}
        AND {{ incremental_predicate('call_block_time') }}
        {% endif %}

    UNION ALL

    --Init v2
    SELECT
        CAST(DATE_TRUNC('day', call_block_time) as DATE) AS block_date
        , call_block_time AS block_time
        , account_account AS token_account
        , SUBSTRING(account_account, 1, 2) AS token_account_prefix
        , 'init' AS event_type
        , owner as account_owner
        , account_mint
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
        {% if is_incremental() %}
        AND {{ incremental_predicate('call_block_time') }}
        {% endif %}

    UNION ALL

    --Init v3
    SELECT
        CAST(DATE_TRUNC('day', call_block_time) as DATE) AS block_date
        , call_block_time AS block_time
        , account_account AS token_account
        , SUBSTRING(account_account, 1, 2) AS token_account_prefix
        , 'init' AS event_type
        , owner as account_owner
        , account_mint
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
        {% if is_incremental() %}
        AND {{ incremental_predicate('call_block_time') }}
        {% endif %}
)
, owner_change as (
    -- Owner Changes: Only owner changes, mint persists, mint address not in data
    SELECT
        CAST(DATE_TRUNC('day', call_block_time) as DATE) AS block_date
        , call_block_time AS block_time
        , account_owned AS token_account
        , SUBSTRING(account_owned, 1, 2) AS token_account_prefix
        , 'owner_change' AS event_type
        , newAuthority as account_owner
        , null as account_mint
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
        {% if is_incremental() %}
        AND {{ incremental_predicate('call_block_time') }}
        {% endif %}
)
, close as (
    --Closure events only contain the token account address
    SELECT
        CAST(DATE_TRUNC('day', call_block_time) as DATE) AS block_date
        , call_block_time AS block_time
        , account_account AS token_account
        , SUBSTRING(account_account, 1, 2) AS token_account_prefix
        , 'close' AS event_type
        , null as account_owner
        , null as account_mint
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
        {% if is_incremental() %}
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
select * from raw_events