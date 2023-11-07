{{ config(
    schema = 'gyroscope_polygon',
    alias = 'trades',
    partition_by = ['block_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'blockchain', 'project', 'version', 'tx_hash', 'evt_index'],
    post_hook = '{{ expose_spells(\'["polygon"]\',
                                "project",
                                "gyroscope",
                                \'["fmarrr"]\') }}'
    )
}}

with E_CLPs AS (
    SELECT 
        x.pool
        , y.min_block_time
        FROM  {{ source('gyroscope_polygon','GyroECLPPoolFactory_evt_PoolCreated') }} x
        LEFT JOIN (
            SELECT min(evt_block_time) AS min_block_time 
            FROM {{ source('gyroscope_polygon','GyroECLPPoolFactory_evt_PoolCreated') }} 
        ) y
        on x.pool is not null
    )

    SELECT
    *,
    'gyroscope' AS project
    FROM {{ ref('balancer_v2_polygon_trades') }} x
    inner join E_CLPs y
    on x.block_time >= y.min_block_time
    and x.project_contract_address = y.pool