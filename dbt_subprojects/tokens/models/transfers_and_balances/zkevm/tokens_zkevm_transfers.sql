{{config(
    schema = 'tokens_zkevm',
    alias = 'transfers',
    partition_by = ['block_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date','unique_key'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_date')],
    post_hook='{{ expose_spells(blockchains = \'["zkevm"]\',
                                spell_type = "sector",
                                spell_name = "tokens",
                                contributors = \'["aalan3", "jeff-dude", "hildobby"]\') }}'
)
}}

{{
    transfers_enrich(
        base_transfers = ref('tokens_zkevm_base_transfers')
        , tokens_erc20_model = source('tokens', 'erc20')
        , prices_model = source('prices', 'usd')
        , evms_info_model = source('evms','info')
        , transfers_start_date = '2023-03-24'
        , blockchain = 'zkevm'
    )
}}
