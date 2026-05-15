{{
    config(
        schema = 'nad_fun_v3_monad',
        alias = 'base_trades',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['tx_hash', 'evt_index'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

-- nad.fun is a memecoin launchpad on Monad. Each token launched on the
-- platform graduates from a bonding curve into a Uniswap V3-style pool
-- (one pool per token, always paired against WMON). The set of pool
-- addresses is enumerated from CurveGraduate(token, pool) events emitted
-- by the BondingCurve contract; the V3-style Swap event then drives the
-- trade rows. This avoids being tied to any particular router/relayer
-- (Metamask delegation manager, nad.fun DEX router, MEV bot, etc.).
--
-- Bonding curve:        0xA7283d07812a02AFB7C09B60f8896bCEA3F90aCE
-- CurveGraduate topic0: 0xa1cae252e597e19f398a442722a17a17e62d17f9d4f3656786e18aabcd428908
-- V3 Swap topic0:       0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67
-- WMON:                 0x3bd359C1119dA7Da1D913D1C4D2B7c461115433A

WITH graduated_pools AS (
    SELECT
        bytearray_substring(topic1, 13, 20) AS token_address,
        bytearray_substring(topic2, 13, 20) AS pool
    FROM {{ source('monad', 'logs') }}
    WHERE contract_address = 0xa7283d07812a02afb7c09b60f8896bcea3f90ace
      AND topic0 = 0xa1cae252e597e19f398a442722a17a17e62d17f9d4f3656786e18aabcd428908
      AND block_date >= DATE '2025-11-01'  -- bonding curve had no graduations before this date
),

swaps AS (
    SELECT
        s.block_time,
        s.block_number,
        s.tx_hash,
        s.index AS evt_index,
        s.contract_address AS pool,
        bytearray_to_int256(bytearray_substring(s.data, 1, 32))  AS amount0,
        bytearray_to_int256(bytearray_substring(s.data, 33, 32)) AS amount1,
        bytearray_substring(s.topic2, 13, 20) AS recipient
    FROM {{ source('monad', 'logs') }} s
    INNER JOIN graduated_pools gp
        ON gp.pool = s.contract_address
    WHERE s.topic0 = 0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67
      AND s.block_date >= DATE '2025-11-01'
    {% if is_incremental() %}
      AND {{ incremental_predicate('s.block_time') }}
    {% endif %}
)

SELECT
    'monad' AS blockchain,
    'nad_fun' AS project,
    '1' AS version,
    CAST(date_trunc('month', s.block_time) AS date) AS block_month,
    CAST(date_trunc('day', s.block_time) AS date)   AS block_date,
    s.block_time,
    s.block_number,
    -- token0 is the lower address. amount0 < 0 means token0 left the pool (trader bought token0).
    CAST(
        CASE
            WHEN gp.token_address < 0x3bd359c1119da7da1d913d1c4d2b7c461115433a
                THEN CASE WHEN s.amount0 < INT256 '0' THEN abs(s.amount0) ELSE abs(s.amount1) END
            ELSE
                CASE WHEN s.amount1 < INT256 '0' THEN abs(s.amount1) ELSE abs(s.amount0) END
        END
        AS UINT256
    ) AS token_bought_amount_raw,
    CAST(
        CASE
            WHEN gp.token_address < 0x3bd359c1119da7da1d913d1c4d2b7c461115433a
                THEN CASE WHEN s.amount0 < INT256 '0' THEN abs(s.amount1) ELSE abs(s.amount0) END
            ELSE
                CASE WHEN s.amount1 < INT256 '0' THEN abs(s.amount0) ELSE abs(s.amount1) END
        END
        AS UINT256
    ) AS token_sold_amount_raw,
    CASE
        WHEN gp.token_address < 0x3bd359c1119da7da1d913d1c4d2b7c461115433a
            THEN CASE WHEN s.amount0 < INT256 '0' THEN gp.token_address ELSE 0x3bd359c1119da7da1d913d1c4d2b7c461115433a END
        ELSE
            CASE WHEN s.amount1 < INT256 '0' THEN gp.token_address ELSE 0x3bd359c1119da7da1d913d1c4d2b7c461115433a END
    END AS token_bought_address,
    CASE
        WHEN gp.token_address < 0x3bd359c1119da7da1d913d1c4d2b7c461115433a
            THEN CASE WHEN s.amount0 < INT256 '0' THEN 0x3bd359c1119da7da1d913d1c4d2b7c461115433a ELSE gp.token_address END
        ELSE
            CASE WHEN s.amount1 < INT256 '0' THEN 0x3bd359c1119da7da1d913d1c4d2b7c461115433a ELSE gp.token_address END
    END AS token_sold_address,
    s.recipient AS taker,
    CAST(NULL AS varbinary) AS maker,
    s.pool AS project_contract_address,
    s.tx_hash,
    s.evt_index
FROM swaps s
INNER JOIN graduated_pools gp ON gp.pool = s.pool
