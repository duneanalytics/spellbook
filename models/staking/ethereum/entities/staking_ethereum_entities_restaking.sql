{{ config(
    schema = 'staking_ethereum',
    alias = 'entities_restaking',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['depositor_address'])
}}

SELECT eigenPod AS depositor_address
, 'EigenLayer' AS entity
, CONCAT('EigenLayer ', CAST(ROW_NUMBER() OVER (ORDER BY MIN(t.block_time)) AS VARCHAR)) AS entity_unique_name
, 'Restaking' AS category
FROM {{ source('eigenlayer_ethereum', 'EigenPodManager_evt_PodDeployed') }}
WHERE eigenPod NOT IN (SELECT depositor_address FROM {{ ref('staking_ethereum_entities_depositor_addresses') }})
{% if is_incremental() %}
AND {{ incremental_predicate('evt_block_time') }}
{% endif %}