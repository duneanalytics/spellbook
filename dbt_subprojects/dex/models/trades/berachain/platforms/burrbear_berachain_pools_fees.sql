{{
    config(
        schema = 'burrbear_berachain',
        alias = 'pools_fees',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['contract_address', 'block_number', 'index'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

WITH pools AS (
    SELECT
        poolAddress AS contract_address,
        evt_block_number AS block_number,
        evt_index AS index,
        evt_block_time AS block_time,
        CASE 
            WHEN specialization = 0 THEN 0.003  -- General Pool
            WHEN specialization = 1 THEN 0.002  -- Minimal Swap Info
            WHEN specialization = 2 THEN 0.001  -- Two Token
            ELSE 0.003  -- Default to 0.3%
        END AS swap_fee_percentage
    FROM {{ source('burrbear_berachain', 'vault_evt_poolregistered') }}
    {% if is_incremental() %}
    WHERE {{ incremental_predicate('evt_block_time') }}
    {% endif %}
)

SELECT * FROM pools
