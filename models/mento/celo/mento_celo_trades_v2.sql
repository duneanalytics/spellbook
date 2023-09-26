{{
  config(
    tags = ['dunesql'],
    schema = 'mento_celo',
    alias = alias('trades_v2'),
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'tx_hash', 'evt_index'],
    post_hook = '{{ expose_spells(\'["celo"]\',
                                "project",
                                "mento",
                                \'["tomfutago"]\') }}'
  )
}}

{{
  trades(
    blockchain = 'celo',
    project = 'mento',
    version = '2',
    project_start_date = '2020-04-22',
    dex = ref('mento_celo_trades_v2_dex'),
    transactions = source('celo', 'transactions')
  )
}}
