{{
    config(
        schema = 'tessera_v_base',
        alias = 'base_trades',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['tx_hash', 'evt_index'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

-- Tessera-V (TesseraSwap) is an EVM proprietary AMM (PMM/PropAMM) — analogous to the Solana
-- TesseraV program (TessVdML9pBGgG9yGks7o4HewRaXVAMuoVj4x83GLQH). On EVM the swap engine
-- is a single vanity-deployed router at 0x55555522005bcae1c2424d474bfd5ed477749e3e that emits
-- one Tesseratrade event per fill, indexed by Dune as
-- tesseraswap_base.tesseraswap_evt_tesseratrade.

WITH dexs AS (
    SELECT
        t.evt_block_number AS block_number,
        t.evt_block_time AS block_time,
        t.recipient AS taker,
        t.contract_address AS maker,
        t.amountOut AS token_bought_amount_raw,
        t.amountIn AS token_sold_amount_raw,
        t.tokenOut AS token_bought_address,
        t.tokenIn AS token_sold_address,
        t.contract_address AS project_contract_address,
        t.evt_tx_hash AS tx_hash,
        t.evt_index
    FROM {{ source('tesseraswap_base', 'tesseraswap_evt_tesseratrade') }} t
    {% if is_incremental() %}
    WHERE {{ incremental_predicate('t.evt_block_time') }}
    {% endif %}
)

SELECT
    'base' AS blockchain,
    'tessera_v' AS project,
    '1' AS version,
    CAST(date_trunc('month', dexs.block_time) AS date) AS block_month,
    CAST(date_trunc('day', dexs.block_time) AS date) AS block_date,
    dexs.block_time,
    dexs.block_number,
    dexs.token_bought_amount_raw,
    dexs.token_sold_amount_raw,
    dexs.token_bought_address,
    dexs.token_sold_address,
    dexs.taker,
    dexs.maker,
    dexs.project_contract_address,
    dexs.tx_hash,
    dexs.evt_index
FROM dexs
