{{ config(
    schema = 'tokens_solana'
    , alias = 'base_transfers_token_account_history'
    , partition_by = ['block_date']
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'append'
    , unique_key = ['block_date', 'unique_instruction_key']
) }}

with base_transfers as (
    select
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
        , source
    from
        {{ ref('tokens_solana_base_transfers') }}
    {% if is_incremental() -%}
    where
        {{ incremental_predicate('block_time') }}
    {% endif -%}
)
, transfers as (
select
    t.block_date
    , t.block_time
    , t.block_slot
    , case
        when t.source = 'sol_transfers'
            then case when tk_to.address is not null then 'wrap' else 'transfer' end -- if the token account exists, it's a wrap, otherwise it's a transfer
        when t.source in ('spl_transfers', 'token22_transfers')
            then t.action
        when t.source = 'spl_transfers_call_transfer'
            then 'transfer'
        end as action
    , t.amount
    , case
        when t.source = 'sol_transfers'
            then 'So11111111111111111111111111111111111111112'
        else
            coalesce(tk_from.token_mint_address, tk_to.token_mint_address)
        end as token_mint_address
    , COALESCE(tk_from.token_balance_owner, t.from_token_account) AS from_owner -- if the token account exists, use the owner of that, otherwise it should be an account
    , COALESCE(tk_to.token_balance_owner, t.to_token_account) AS to_owner -- if the token account exists, use the owner of that, otherwise it should be an account
    , t.from_token_account
    , t.to_token_account
    , t.token_version
    , t.tx_signer
    , t.tx_id
    , t.tx_index
    , t.outer_instruction_index
    , t.inner_instruction_index
    , t.outer_executing_account
    , t.unique_instruction_key
    , t.source
from
    base_transfers as t
left join 
    {{ ref('solana_utils_token_accounts_state_history') }} as tk_from    
    on t.from_token_account_prefix = tk_from.address_prefix
    and t.from_token_account = tk_from.address
    and t.unique_instruction_key >= tk_from.valid_from_unique_instruction_key
    and t.unique_instruction_key < tk_from.valid_to_unique_instruction_key
left join 
    {{ ref('solana_utils_token_accounts_state_history') }} as tk_to 
    on t.to_token_account_prefix = tk_to.address_prefix
    and t.to_token_account = tk_to.address
    and t.unique_instruction_key >= tk_to.valid_from_unique_instruction_key
    and t.unique_instruction_key < tk_to.valid_to_unique_instruction_key
)
, final as (
    select
        t.*
    from
        transfers as t
    {% if is_incremental() -%}
    left join
        {{ this }} as existing
        on existing.block_date = t.block_date
        and existing.unique_instruction_key = t.unique_instruction_key
        and {{ incremental_predicate('existing.block_time') }}
    where
        existing.block_date is null -- only insert new rows
    {% endif -%}
)
select
    *
from
    final