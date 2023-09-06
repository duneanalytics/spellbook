{{
    config(
        tags = ['dunesql'],
        schema = 'curvefi_optimism',
        alias = alias('gauge_mappings'),
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['pool_contract', 'incentives_contract'],
        post_hook='{{ expose_spells(\'["optimism"]\',
                                    "project",
                                    "curvefi",
                                    \'["msilb7"]\') }}'
    ) 
}}

SELECT distinct 
        'optimism' as blockchain
        , '1' as version
        , a.pool AS pool_contract
        , a.gauge AS incentives_contract
        , 'rewards gauge' AS incentives_type
        , a.evt_block_time
        , a.evt_block_number
        , a.contract_address
        , a.evt_tx_hash
        , a.evt_index
FROM
    (
        SELECT 
            _lp_token As pool
            , _gauge AS gauge
            , evt_block_time
            , evt_block_number
            , contract_address
            , evt_tx_hash
            , evt_index
        FROM {{ source ('curvefi_optimism', 'Vyper_contract_evt_DeployedGauge') }}
        {% if is_incremental() %}
        WHERE evt_block_time >= date_trunc('day', now() - interval '7' day)
        {% endif %}

        UNION ALL 

        SELECT 
            pool
            , gauge
            , evt_block_time
            , evt_block_number
            , contract_address
            , evt_tx_hash
            , evt_index
        FROM {{ source ('curvefi_optimism', 'PoolFactory_evt_LiquidityGaugeDeployed') }}
        {% if is_incremental() %}
        WHERE evt_block_time >= date_trunc('day', now() - interval '7' day)
        {% endif %}
) a