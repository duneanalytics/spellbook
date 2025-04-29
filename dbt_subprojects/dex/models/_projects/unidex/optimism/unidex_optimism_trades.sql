{{ config(
    alias = 'trades',
    schema = 'unidex_optimism',
    materialized = 'view',
    post_hook = '{{ expose_spells(blockchains = \'["optimism"]\',
                                spell_type = "project",
                                spell_name = "unidex",
                                contributors = \'["ARDev097", "hosuke"]\') }}'
    )
}}


SELECT *
FROM {{ ref('dex_aggregator_trades') }}
WHERE project = 'unidex'
  AND blockchain = 'optimism'