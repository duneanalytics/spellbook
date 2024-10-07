{{ config(
    alias='trades',
    schema='unidex_optimism',
    partition_by=['block_month'],
    materialized='incremental',
    file_format='delta',
    incremental_strategy='merge',
    unique_key=['block_date', 'blockchain', 'project', 'version', 'tx_hash', 'evt_index'],
    post_hook='{{ expose_spells(blockchains = \'["optimism"]\',
                                spell_type = "project",
                                spell_name = "unidex",
                                contributors = \'["ARDev097", "hosuke"]\') }}'
    )
}}


SELECT *
FROM {{ ref('dex_aggregator_trades') }}
WHERE project = 'unidex'
  AND blockchain = 'optimism'