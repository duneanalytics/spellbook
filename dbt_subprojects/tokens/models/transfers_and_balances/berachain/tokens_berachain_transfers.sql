{{config(
     schema = 'tokens_berachain',
     alias = 'transfers',
     partition_by = ['block_month'],
     materialized = 'incremental',
     file_format = 'delta',
     incremental_strategy = 'merge',
     unique_key = ['block_date','unique_key'],
     incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_date')],
     post_hook='{{ expose_spells(blockchains = \'["berachain"]\',
                                 spell_type = "sector",
                                 spell_name = "tokens",
                                 contributors = \'["hosuke"]\') }}'
  )
}}

-- Berachain mainnet launch date: 2025-01-20 (Block #1)
{{ transfers_enrich(
    base_transfers = ref('tokens_berachain_base_transfers'),
    tokens_erc20_model = source('tokens', 'erc20'),
    prices_model = source('prices', 'hour'),
    transfers_start_date = '2025-01-20',
    blockchain = 'berachain'
  ) 
}}
