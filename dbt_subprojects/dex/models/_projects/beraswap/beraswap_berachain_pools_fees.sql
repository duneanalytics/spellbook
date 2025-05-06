{{
    config(
        schema = 'beraswap_berachain',
        alias = 'pools_fees',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['block_number', 'tx_hash', 'index'],
        post_hook='{{ expose_spells(blockchains = \'["berachain"]\',
                                    spell_type = "project",
                                    spell_name = "beraswap",
                                    contributors = \'["hosuke"]\') }}'
    ) 
}}

WITH pools AS (
    SELECT
        p.poolId AS pool_id,
        p.poolAddress AS pool_address,
        p.specialization,
        p.evt_block_time AS block_time,
        p.evt_block_number AS block_number,
        p.evt_tx_hash AS tx_hash,
        p.evt_index AS index,
        p.evt_tx_index AS tx_index,
        CASE 
            WHEN w.pool IS NOT NULL THEN 0.003  -- Weighted Pool
            WHEN p.specialization = 0 THEN 0.003  -- General Pool
            WHEN p.specialization = 1 THEN 0.002  -- Minimal Swap Info
            WHEN p.specialization = 2 THEN 0.001  -- Two Token
            ELSE 0.003  -- Default to 0.3%
        END AS swap_fee_percentage
    FROM {{ source('beraswap_berachain', 'vault_evt_poolregistered') }} p
    LEFT JOIN {{ source('beraswap_berachain', 'weightedpoolfactory_evt_poolcreated') }} w
        ON p.poolAddress = w.pool
    {% if is_incremental() %}
    WHERE {{ incremental_predicate('p.evt_block_time') }}
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
