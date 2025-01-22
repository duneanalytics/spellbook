{{ config(
    schema = 'staking_ethereum',
    alias = 'entities_stakewise_v3',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['depositor_address'])
}}

SELECT vault AS depositor_address
, 'StakeWise' AS entity
, CONCAT('StakeWise v3 Vault ', CAST(ROW_NUMBER() OVER (ORDER BY MIN(evt_block_time)) AS VARCHAR)) AS entity_unique_name
, 'Staking Pool' AS category
FROM {{ source('stakewise_v3_ethereum', 'VaultsRegistry_evt_VaultAdded') }}
{% if is_incremental() %}
WHERE evt_block_time >= date_trunc('day', now() - interval '7' day)
{% endif %}
GROUP BY 1