{{config(
    schema = 'tokens_ink'
    , alias = 'base_transfers'
    , partition_by = ['block_month']
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    , unique_key = ['block_date','unique_key']
)
}}

{{transfers_base(
    blockchain='ink'
    , traces = source('ink','traces')
    , transactions = source('ink','transactions')
    , erc20_transfers = source('erc20_ink','evt_transfer')
    , native_contract_address = var('ETH_ERC20_ADDRESS')
)
}} 