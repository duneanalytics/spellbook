{{ 
    config(
        materialized='incremental',
        schema = 'safe_optimism',
        alias = 'eth_transfers',
        partition_by = ['block_month'],
        unique_key = ['block_date', 'address', 'tx_hash', 'trace_address'],
        on_schema_change='fail',
        file_format ='delta',
        incremental_strategy='merge',
        post_hook='{{ expose_spells(blockchains = \'["optimism"]\',
                                    spell_type = "project",
                                    spell_name = "safe",
                                    contributors = \'["tschubotz", "hosuke"]\') }}'
    )
}}

{% set project_start_date = '2021-11-17' %}

-- First, get the standard ETH transfers
WITH standard_transfers AS (
    {{
        safe_native_transfers(
            blockchain = 'optimism',
            native_token_symbol = 'ETH',
            project_start_date = project_start_date
        )
    }}
),

-- Then, get the special ERC20 transfers for 'deadeadead' ETH token
erc20_eth_transfers AS (
    SELECT
        'optimism' as blockchain,
        'ETH' as symbol,
        s.address,
        try_cast(date_trunc('day', r.evt_block_time) as date) as block_date,
        CAST(date_trunc('month', r.evt_block_time) as DATE) as block_month,
        r.evt_block_time as block_time,
        CAST(r.value AS INT256) as amount_raw,
        r.evt_tx_hash as tx_hash,
        cast(r.evt_index as varchar) as trace_address,
        'erc20' as transfer_type
    FROM {{ source('erc20_optimism', 'evt_Transfer') }} r
    INNER JOIN {{ ref('safe_optimism_safes') }} s
        ON r.to = s.address
    WHERE
        r.contract_address = 0xdeaddeaddeaddeaddeaddeaddeaddeaddead0000
        AND r.value > UINT256 '0'
        {% if not is_incremental() %}
        AND r.evt_block_time > TIMESTAMP '{{project_start_date}}'
        {% endif %}
        {% if is_incremental() %}
        AND r.evt_block_time >= date_trunc('day', now() - interval '10' day)
        {% endif %}

    UNION ALL

    SELECT
        'optimism' as blockchain,
        'ETH' as symbol,
        s.address,
        try_cast(date_trunc('day', r.evt_block_time) as date) as block_date,
        CAST(date_trunc('month', r.evt_block_time) as DATE) as block_month,
        r.evt_block_time as block_time,
        -CAST(r.value AS INT256) as amount_raw,
        r.evt_tx_hash as tx_hash,
        cast(r.evt_index as varchar) as trace_address,
        'erc20' as transfer_type
    FROM {{ source('erc20_optimism', 'evt_Transfer') }} r
    INNER JOIN {{ ref('safe_optimism_safes') }} s
        ON r."from" = s.address
    WHERE
        r.contract_address = 0xdeaddeaddeaddeaddeaddeaddeaddeaddead0000
        AND r.value > UINT256 '0'
        {% if not is_incremental() %}
        AND r.evt_block_time > TIMESTAMP '{{project_start_date}}'
        {% endif %}
        {% if is_incremental() %}
        AND r.evt_block_time >= date_trunc('day', now() - interval '10' day)
        {% endif %}
),

-- Combine both types of transfers
combined_transfers AS (
    SELECT
        *,
        'standard' as transfer_type
    FROM standard_transfers
    UNION ALL
    SELECT * FROM erc20_eth_transfers
)

-- Final select with price data
SELECT
    t.*,
    p.price * t.amount_raw / 1e18 AS amount_usd
FROM combined_transfers t
LEFT JOIN {{ source('prices', 'usd') }} p
    ON p.blockchain IS NULL
    AND p.symbol = t.symbol
    AND p.minute = date_trunc('minute', t.block_time)
