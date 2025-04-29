{{ config(
    schema = 'tokens_solana',
    alias = 'base_transfers',
    partition_by = ['block_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'append',
    unique_key = ['block_date', 'unique_instruction_key']
) }}

{%- set models = [
    'tokens_solana_base_sol_transfers'
    , 'tokens_solana_base_spl_transfers'
    , 'tokens_solana_base_spl_transfers_call_transfer'
    , 'tokens_solana_base_token22_spl_transfers'
] -%}

with base_transfers as (
    {% for model in models -%}
    SELECT
        block_date
        , block_time
        , block_slot
        , action
        , tx_id
        , tx_index
        , inner_instruction_index
        , outer_instruction_index
        , tx_signer
        , amount
        , outer_executing_account
        , from_token_account_prefix
        , from_token_account
        , to_token_account_prefix
        , to_token_account
        , token_version
        , unique_instruction_key
        , replace('{{model}}', 'tokens_solana_base_', '') as source
    FROM 
        {{ ref(model) }}
    {% if is_incremental() or true -%}
    WHERE
        {{incremental_predicate('block_time')}}
    {% endif -%}
    {% if not loop.last -%}
    UNION ALL
    {% endif -%}
    {% endfor -%}
)
, final as (
    select
        t.*
    from
        base_transfers as t
    {% if is_incremental() -%}
    left join
        {{ this }} as existing
        on existing.block_date = t.block_date
        and existing.unique_instruction_key = t.unique_instruction_key
        and {{incremental_predicate('existing.block_time')}}
    where
        existing.block_date is null -- only insert new rows
    {% endif -%}
)
select
    *
from
    final