{{ config(
    schema = 'gyroscope_ethereum',
    tags = ['dunesql'],
    alias = 'trades',
    partition_by = ['block_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'blockchain', 'project', 'version', 'tx_hash', 'evt_index'],
    post_hook = '{{ expose_spells(\'["ethereum"]\',
                                "project",
                                "gyroscope",
                                \'["fmarrr"]\') }}'
    )
}}

{% set project_start_date = '2021-04-20' %}


with E_CLPs AS (
    SELECT
       pool
    from {{ source('gyroscope_ethereum','GyroECLPPoolFactory_evt_PoolCreated') }}
)

SELECT
*
FROM ref ('balancer_trades')
where project_contract_address IN (SELECT pool FROM E_CLPs)