{{ config(
    alias='trades',
    schema='firebird_finance_optimism',
    materialized='view',
    post_hook='{{ expose_spells(blockchains = \'["optimism"]\',
                                spell_type = "project",
                                spell_name = "firebird_finance",
                                contributors = \'["ARDev097", "hosuke"]\') }}'
    )
}}

SELECT *
FROM {{ ref('dex_aggregator_trades') }}
WHERE project = 'firebird_finance'
  AND blockchain = 'optimism'
