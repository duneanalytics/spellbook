{{config(
    schema = 'tokens_tempo'
    , alias = 'transfers'
    , partition_by = ['block_month']
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , merge_skip_unchanged = true
    , unique_key = ['block_date','unique_key']
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    , post_hook='{{ hide_spells() }}'
    )
}}

with enriched as (
    {{
        transfers_enrich(
            base_transfers = ref('tokens_tempo_base_transfers')
            , transfers_start_date = '2026-01-16'
            , blockchain = 'tempo'
        )
    }}
)

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
    , "to"
    , contract_address
    , symbol
    , amount_raw
    , amount
    , case
        when contract_address = 0x20c0000000000000000000000000000000000000 then coalesce(price_usd, 1.0)
        else price_usd
    end as price_usd
    , case
        when contract_address = 0x20c0000000000000000000000000000000000000 then coalesce(amount_usd, amount * 1.0)
        else amount_usd
    end as amount_usd
    , _updated_at
from enriched
