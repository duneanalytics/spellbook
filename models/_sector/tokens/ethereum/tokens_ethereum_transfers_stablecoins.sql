{{
    config(
        schema = 'tokens_ethereum',
        alias = 'transfers_stablecoins',
        partition_by = ['block_date'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['block_date', 'tx_hash', 'contract_address'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_date')],
        post_hook='{{ expose_spells(blockchains = \'["ethereum"]\',
                                    spell_type = "sector",
                                    spell_name = "tokens",
                                    contributors = \'["thetroyharris"]\') }}'
    )
}}

{{ 
    transfers_erc20_stablecoins(
        blockchain = 'ethereum'
        , first_stablecoin_deployed = '2017-11-28' 
    )
}}
