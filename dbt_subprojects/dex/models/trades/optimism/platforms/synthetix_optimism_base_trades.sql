{{
    config(
        schema = 'synthetix_optimism',
        alias = 'base_trades',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['tx_hash', 'evt_index'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

WITH dexs AS (
    SELECT
        t.evt_block_number AS block_number,
        t.evt_block_time AS block_time,
        t.toAddress AS taker,
        CAST(NULL AS VARBINARY) AS maker,
        t.toAmount AS token_bought_amount_raw,
        t.fromAmount AS token_sold_amount_raw,
        CAST(NULL AS VARBINARY) AS token_bought_address,
        CAST(NULL AS VARBINARY) AS token_sold_address,
        from_utf8(bytearray_rtrim(t.toCurrencyKey)) AS token_bought_symbol,
        from_utf8(bytearray_rtrim(t.fromCurrencyKey)) AS token_sold_symbol,
        t.contract_address AS project_contract_address,
        t.evt_tx_hash AS tx_hash,
        t.evt_index
    FROM {{ source('synthetix_optimism', 'SNX_evt_SynthExchange') }} t
    {% if is_incremental() %}
    WHERE {{ incremental_predicate('t.evt_block_time') }}
    {% endif %}
),

currency_keys as (
select 
    currencyKey, synth as synth_address, evt_block_time as valid_from
    ,lead(evt_block_time) over (partition by currencyKey order by evt_block_time) as valid_to
from {{ source('synthetix_optimism', 'issuer_evt_synthadded') }}
)

SELECT
    'optimism' AS blockchain,
    'synthetix' AS project,
    '1' AS version,
    CAST(date_trunc('month', dexs.block_time) AS date) AS block_month,
    CAST(date_trunc('day', dexs.block_time) AS date) AS block_date,
    dexs.block_time,
    dexs.block_number,
    dexs.token_bought_amount_raw,
    dexs.token_sold_amount_raw,
    erc20_bought.contract_address AS token_bought_address,
    erc20_sold.contract_address AS token_sold_address,
    dexs.taker,
    dexs.maker,
    dexs.project_contract_address,
    dexs.tx_hash,
    dexs.evt_index
FROM dexs
INNER JOIN currency_keys
    ON currency_keys.currencyKey = dexs.token_bought_symbol
    AND dexs.block_time between currency_keys.valid_from and currency_keys.valid_to
LEFT JOIN {{ source('tokens', 'erc20') }} erc20_bought
    ON erc20_bought.contract_address = currency_keys.synth_address
    AND erc20_bought.blockchain = 'optimism'
LEFT JOIN {{ source('tokens', 'erc20') }} erc20_sold
    ON erc20_sold.contract_address = currency_keys.synth_address
    AND erc20_sold.blockchain = 'optimism'
