{{ config(
    schema = 'aerodrome',
    alias = 'pool_fees',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_number', 'pool', 'block_time'],
    post_hook='{{ expose_spells(blockchains = \'["base"]\',
                                  spell_type = "project", 
                                  spell_name = "aerodrome", 
                                  contributors = \'["BroderickBonelli"]\') }}'
    )
}}

WITH cl_pool_fee_per_block AS(
    SELECT 
        pool, 
        call_block_number AS block_number,
        call_block_time AS block_time,
        MAX(output_0) AS fee
    FROM {{ source('aerodrome_base', 'CLFactory_call_getSwapFee') }}
    WHERE call_success = True
    {% if is_incremental() %}
        AND {{ incremental_predicate('call_block_time') }}
    {% endif %}
    GROUP BY 1,2,3
),

non_cl_pool_fee_per_block AS(
    SELECT 
        pool, 
        call_block_number AS block_number,
        call_block_time AS block_time,
        MAX(output_0) AS fee
    FROM {{ source('aerodrome_base', 'poolfactory_call_getfee') }}
    WHERE call_success = True
    {% if is_incremental() %}
        AND {{ incremental_predicate('call_block_time') }}
    {% endif %}
    GROUP BY 1,2,3
)

SELECT * FROM cl_pool_fee_per_block
UNION ALL
SELECT * FROM non_cl_pool_fee_per_block