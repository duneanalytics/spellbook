{{ config(
        schema = 'tokens_bitcoin'
        , alias = 'net_transfers_daily_asset'
        , materialized = 'incremental'
        , file_format = 'delta'
        , incremental_strategy = 'merge'
        , unique_key = ['blockchain', 'block_date', 'contract_address']
        , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_date')]
)
}}

select
    blockchain
    , block_date
    , cast('0x0000000000000000000000000000000000' as varchar(42)) as contract_address
    , 'BTC' as symbol
    , net_transfer_amount_usd
from
    {{ ref('tokens_bitcoin_net_transfers_daily') }}
{% if is_incremental() %}
where
    {{ incremental_predicate('block_date') }}
{% endif %}
