{{ config(
        
        schema = 'yearn'
        , alias = 'vaults'
        , materialized = 'incremental'
        , file_format = 'delta'
        , incremental_strategy = 'merge'
        , unique_key = ['vault_token']
        , post_hook='{{ expose_spells(\'["optimism"]\',
                                  "project",
                                  "yearn",
                                  \'["msilb7"]\') }}'
  )
}}

SELECT
  'optimism' as blockchain
, call_block_time AS block_time_created
, call_block_number AS block_number_created
, _token AS underlying_token
, _symbol AS vault_symbol
, _name AS vault_name
, output_0 AS vault_token

FROM {{ source('yearn_optimism', 'ReleaseRegistry_call_newVault') }}

WHERE call_success = true
{% if is_incremental() %}
AND call_block_time >= date_trunc('day', now() - interval '7' day)
{% endif %}
