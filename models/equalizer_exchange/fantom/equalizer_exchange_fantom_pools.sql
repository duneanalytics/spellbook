{{ config(
	tags=['legacy'],
	
    alias = alias('pools', legacy_model=True),
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['pool'],
    post_hook='{{ expose_spells(\'["fantom"]\',
                                "project",
                                "equalizer_exchange",
                                \'["Henrystats"]\') }}'
    )
}}

SELECT 'fantom' AS blockchain
, 'equalizer_exchange' AS project
, '1' AS version
, pair AS pool
, CASE WHEN stable = true THEN 0.02 ELSE 0.2 END as fee 
, token0
, token1
, evt_block_time AS creation_block_time
, evt_block_number AS creation_block_number
, contract_address
FROM {{ source('equalizer_exchange_fantom', 'PairFactory_evt_PairCreated') }}
{% if is_incremental() %}
WHERE evt_block_time >= date_trunc("day", now() - interval '1 week')
{% endif %}