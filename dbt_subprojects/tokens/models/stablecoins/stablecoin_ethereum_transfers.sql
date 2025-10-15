{{ config(
    schema = 'stablecoin_ethereum',
    alias = 'transfers',
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    unique_key = ['block_date','unique_key'],
    post_hook='{{ expose_spells(\'["ethereum"]\',
                                "sector",
                                "stablecoin",
                                \'["0xBoxer"]\') }}'
    )
}}

-- Stablecoin transfers for Ethereum
-- This model filters tokens_ethereum_transfers to only include stablecoin tokens
-- defined in tokens_ethereum_erc20_stablecoins

WITH stablecoin_list AS (
    SELECT DISTINCT
        s.contract_address,
        s.symbol,
        s.backing,
        s.decimals,
        s.name,
        s.denomination
    FROM {{ ref('tokens_ethereum_erc20_stablecoins') }} s
)

SELECT
    t.unique_key,
    t.blockchain,
    t.block_month,
    t.block_date,
    t.block_time,
    t.block_number,
    t.tx_hash,
    t.evt_index,
    t.trace_address,
    t.token_standard,
    t.tx_from,
    t.tx_to,
    t.tx_index,
    t."from",
    t."to",
    t.contract_address,
    t.symbol,
    t.amount_raw,
    t.amount,
    s.backing,
    s.name AS stablecoin_name,
    s.denomination
FROM {{ ref('tokens_ethereum_transfers') }} t 
INNER JOIN stablecoin_list s
    ON t.contract_address = s.contract_address
WHERE block_date >=  DATE ('2025-10-10')
{% if is_incremental() %}
WHERE {{ incremental_predicate('t.block_time') }}
{% endif %}

