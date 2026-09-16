{% set blockchain = 'arc' %}

{{ config(
        schema = 'tokens_' + blockchain
        , alias = 'net_transfers_daily_asset'
        , materialized = 'incremental'
        , file_format = 'delta'
        , incremental_strategy = 'merge'
        , unique_key = ['blockchain', 'block_date', 'contract_address']
        , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_date')]
        )
}}

-- Arc's native asset sits at 0x0000...0000 (dune.blockchains' token_address for the chain),
-- which is the same address ETH_ERC20_ADDRESS carries, so the native coalesce below is a
-- no-op here rather than a substitution.
{{ evm_net_transfers_daily_asset(
        blockchain = blockchain,
        native_contract_address = var('ETH_ERC20_ADDRESS')
)
}}
