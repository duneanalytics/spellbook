{{ config(
    schema = 'tokens_solana'
    , alias = 'sol_transfers'
    , partition_by = ['block_date']
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , unique_key = ['block_date', 'unique_instruction_key']

) }}

with base_sol_transfers as (
    select
        blockchain
        , block_date
        , block_time
        , block_slot
        , tx_id
        , tx_index
        , inner_instruction_index
        , outer_instruction_index
        , tx_signer
        , amount
        , outer_executing_account
        , inner_executing_account
        , from_token_account_prefix
        , from_token_account
        , to_token_account_prefix
        , to_token_account
        , token_version
        , unique_instruction_key
    from
        {{ ref('tokens_solana_base_sol_transfers') }}
    {% if is_incremental() -%}
    where
        {{ incremental_predicate('block_time') }}
    {% endif -%}
)
, prices AS (
    select
        minute
        , price
    from
        {{ source('prices', 'usd_forward_fill') }}
    where
        blockchain = 'solana'
        and contract_address =  0x069b8857feab8184fb687f634618c035dac439dc1aeb3b5598a0f00000000001 -- SOL address
        and minute >= TIMESTAMP '2020-10-02 00:00' --solana start date
        {% if is_incremental() or true -%}
        and {{incremental_predicate('minute')}}
        {% else -%}
        and minute >= {{start_date}}
        and minute < {{end_date}}
        {% endif -%}
)
select
    t.block_date
    , t.block_time
    , t.block_slot
    , CASE WHEN tk_to.address IS NOT NULL THEN 'wrap' ELSE 'transfer' END as action -- if the token account exists, it's a wrap, otherwise it's a transfer
    , t.amount
    , t.amount / 1e9 as amount_display
    , p.price * (t.amount / 1e9) as amount_usd
    , p.price as price_usd
    , 'So11111111111111111111111111111111111111112' as token_mint_address
    , 'SOL' as symbol
    , COALESCE(tk_from.token_balance_owner, t.from_token_account) AS from_owner -- if the token account exists, use the owner of that, otherwise it should be an account
    , COALESCE(tk_to.token_balance_owner, t.to_token_account) AS to_owner
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
from
    base_sol_transfers as t
left join prices as p
    on p.minute = date_trunc('minute', t.block_time)
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