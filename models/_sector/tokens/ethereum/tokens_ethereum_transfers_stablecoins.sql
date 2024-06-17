{{
    config(
        schema = 'tokens_ethereum',
        alias = 'transfers_stablecoins',
        partition_by = ['evt_block_date'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['evt_block_date', 'evt_tx_hash', 'contract_address'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.evt_block_date')],
        post_hook='{{ expose_spells(blockchains = \'["ethereum"]\',
                                    spell_type = "sector",
                                    spell_name = "tokens",
                                    contributors = \'["thetroyharris"]\') }}'
    )
}}

{{ transfers_erc20_stablecoins(blockchain = 'ethereum') }}
