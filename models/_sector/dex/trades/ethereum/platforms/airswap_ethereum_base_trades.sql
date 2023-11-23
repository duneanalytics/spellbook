{{ config(
    schema = 'airswap_ethereum'
    , alias ='base_trades'
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , unique_key = ['tx_hash', 'evt_index']
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

WITH dexs AS
(
    SELECT
        evt_block_time AS block_time,
        'light' AS version,
        e.senderWallet AS taker,
        e.signerWallet AS maker,
        e.senderAmount AS token_sold_amount_raw,
        e.signerAmount AS token_bought_amount_raw,
        e.senderToken AS token_sold_address,
        e.signerToken AS token_bought_address,
        contract_address AS project_contract_address,
        evt_tx_hash AS tx_hash,
        evt_block_number AS block_number,
        evt_index
    FROM {{ source('airswap_ethereum', 'Light_evt_Swap')}} e
    {% if is_incremental() %}
    WHERE {{incremental_predicate('evt_block_time')}}
    {% endif %}

    UNION ALL

    SELECT
        evt_block_time AS block_time,
        'light_v0' AS version,
        e.senderWallet AS taker,
        e.signerWallet AS maker,
        e.senderAmount AS token_sold_amount_raw,
        e.signerAmount AS token_bought_amount_raw,
        e.senderToken AS token_sold_address,
        e.signerToken AS token_bought_address,
        contract_address AS project_contract_address,
        evt_tx_hash AS tx_hash,
        evt_block_number AS block_number,
        evt_index
    FROM {{ source('airswap_ethereum', 'Light_v0_evt_Swap')}} e
    {% if is_incremental() %}
    WHERE {{incremental_predicate('evt_block_time')}}
    {% endif %}

    UNION ALL

    SELECT
        evt_block_time AS block_time,
        'swap' AS version,
        e.senderWallet AS taker,
        e.signerWallet AS maker,
        e.senderAmount AS token_sold_amount_raw,
        e.signerAmount AS token_bought_amount_raw,
        e.senderToken AS token_sold_address,
        e.signerToken AS token_bought_address,
        contract_address AS project_contract_address,
        evt_tx_hash AS tx_hash,
        evt_block_number AS block_number,
        evt_index
    FROM {{ source('airswap_ethereum', 'swap_evt_Swap') }} e
    {% if is_incremental() %}
    WHERE {{incremental_predicate('evt_block_time')}}
    {% endif %}

    UNION ALL

    SELECT
        evt_block_time AS block_time,
        'swap_v3' AS version,
        e.senderWallet AS taker,
        e.signerWallet AS maker,
        e.senderAmount AS token_sold_amount_raw,
        e.signerAmount AS token_bought_amount_raw,
        e.senderToken AS token_sold_address,
        e.signerToken AS token_bought_address,
        contract_address AS project_contract_address,
        evt_tx_hash AS tx_hash,
        evt_block_number AS block_number,
        evt_index
    FROM {{ source('airswap_ethereum', 'Swap_v3_evt_Swap')}} e
    {% if is_incremental() %}
    WHERE {{incremental_predicate('evt_block_time')}}
    {% endif %}

    UNION ALL

    SELECT
        evt_block_time AS block_time,
        'swap_erc20_v4' AS version,
        e.senderWallet AS taker,
        e.signerWallet AS maker,
        e.senderAmount AS token_sold_amount_raw,
        e.signerAmount AS token_bought_amount_raw,
        e.senderToken AS token_sold_address,
        e.signerToken AS token_bought_address,
        contract_address AS project_contract_address,
        evt_tx_hash AS tx_hash,
        evt_index
    FROM {{ source('airswap_ethereum', 'SwapERC20_v4_evt_SwapERC20')}} e
    {% if is_incremental() %}
    WHERE {{incremental_predicate('evt_block_time')}}
    {% endif %}
)

SELECT
    'ethereum' AS blockchain
    , 'airswap' AS project
    , version
    , CAST(date_trunc('month', dexs.block_time) AS date) AS block_month
    , CAST(date_trunc('day', dexs.block_time) AS date) AS block_date
    , dexs.block_time
    , dexs.block_number
    , dexs.token_bought_amount_raw
    , dexs.token_sold_amount_raw
    , dexs.token_bought_address
    , dexs.token_sold_address
    , dexs.taker
    , dexs.maker
    , dexs.project_contract_address
    , dexs.tx_hash
    , dexs.evt_index
FROM
    dexs