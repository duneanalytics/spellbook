{{ config(
    schema = 'tokens_solana',
    alias = 'transfers',
    partition_by = ['block_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'append',
    unique_key = ['block_date', 'unique_instruction_key'],
    post_hook='{{ expose_spells(\'["solana"]\',
                                "sector",
                                "tokens_solana",
                                \'["ilemi", "0xBoxer", "jeff-dude"]\') }}'
) }}

with base_transfers as (
    select
        block_date
        , block_time
        , block_slot
        , action
        , amount
        , token_mint_address
        , from_owner
        , to_owner
        , from_token_account
        , to_token_account
        , token_version
        , tx_signer
        , tx_id
        , tx_index
        , outer_instruction_index
        , inner_instruction_index
        , outer_executing_account
        , unique_instruction_key
        , source
    from
        {{ ref('tokens_solana_base_transfers_token_account_history') }}
    {% if is_incremental() -%}
    where
        {{ incremental_predicate('block_time') }}
    {% endif -%}
)
, prices as (
    select
        contract_address
        , minute
        , price
        , decimals
        , symbol
    from
        {{ source('prices', 'usd_forward_fill') }}
    where
        blockchain = 'solana'
        {% if is_incremental() -%}
        and {{incremental_predicate('minute')}}
        {% else -%}
        and minute >= TIMESTAMP '2020-10-02 00:00' --solana start date
        {% endif -%}
)
, transfers as (
    select
        cast(date_trunc('month', t.block_time) as date) as block_month
        , t.block_date
        , cast(date_trunc('hour', t.block_time) as timestamp) as block_hour
        , t.block_time
        , t.block_slot
        , t.action
        , t.amount
        , case
            when t.source = 'sol_transfers'
                then t.amount / 1e9 --only native, can hardcode decimals
            else
                case  
                    when metadata.decimals is null then null
                    when metadata.decimals = 0 then t.amount
                    else t.amount / power(10, metadata.decimals)
                end
            end as amount_display
        , case
            when t.source = 'sol_transfers'
                then (t.amount / 1e9) * p.price --only native, can hardcode decimals
            else
                case  
                    when metadata.decimals is null then null
                    when metadata.decimals = 0 then t.amount * p.price
                    else (t.amount / power(10, metadata.decimals)) * p.price
                end
            end as amount_usd
        , p.price as price_usd
        , t.token_mint_address
        , case
            when t.source = 'sol_transfers'
                then 'SOL'
            else
                metadata.symbol
            end as symbol
        , t.from_owner
        , t.to_owner
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
        {{ ref('tokens_solana_fungible') }} as metadata
        on t.token_mint_address = metadata.token_mint_address
    left join
        {{ ref('solana_utils_token_address_mapping') }} as tk_m
        on tk_m.base58_address = t.token_mint_address
    left join
        prices as p
        on p.contract_address = tk_m.binary_address
        and p.minute = date_trunc('minute', t.block_time)
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