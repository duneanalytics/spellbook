{{
    config(
        schema = 'burrbear_berachain',
        alias = 'pools_fees',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['block_number', 'tx_hash', 'index'],
        post_hook='{{ expose_spells(blockchains = \'["berachain"]\',
                                    spell_type = "project",
                                    spell_name = "burrbear",
                                    contributors = \'["hosuke"]\') }}'
    ) 
}}

WITH pools AS (
    SELECT
        poolId AS pool_id,
        poolAddress AS pool_address,
        specialization,
        evt_block_time AS block_time,
        evt_block_number AS block_number,
        evt_tx_hash AS tx_hash,
        evt_index AS index,
        evt_tx_index AS tx_index,
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

SELECT
    'berachain' AS blockchain,
    '1' AS version,
    pool_address AS contract_address,
    tx_hash,
    tx_index,
    index,
    block_time,
    block_number,
    swap_fee_percentage
FROM pools
