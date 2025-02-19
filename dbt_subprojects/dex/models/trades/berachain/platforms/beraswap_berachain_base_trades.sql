{{
    config(
        schema = 'beraswap_berachain',
        alias = 'base_trades',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['tx_hash', 'evt_index'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
        post_hook='{{ expose_spells(blockchains = \'["berachain"]\',
                                    spell_type = "project",
                                    spell_name = "beraswap",
                                    contributors = \'["hosuke"]\') }}'
    )
}}

{{
    balancer_compatible_v2_trades(
        blockchain = 'berachain',
        project = 'beraswap',
        version = '1',
        project_decoded_as = 'beraswap',
        Vault_evt_Swap = source('beraswap_berachain', 'vault_evt_swap'),
        pools_fees = ref('beraswap_berachain_pools_fees')
    )
}}
