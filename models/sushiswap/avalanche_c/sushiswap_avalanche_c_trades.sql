{{ config(
    schema = 'sushiswap_avalanche_c'
    ,alias = 'trades'
    ,partition_by = ['block_date']
    ,materialized = 'incremental'
    ,file_format = 'delta'
    ,incremental_strategy = 'merge'
    ,unique_key = ['block_date', 'blockchain', 'project', 'version', 'tx_hash', 'evt_index', 'trace_address']
    ,post_hook='{{ expose_spells(\'["avalanche_c"]\',
                                      "project",
                                      "sushiswap",
                                    \'["hosuke", "zhongyiio"]\') }}'
    )
}}

{% set project_start_date = '2022-01-07' %}

WITH sushiswap_decodes AS (

    SELECT
        call_block_number,
        call_block_time,
        call_trace_address,
        call_tx_hash,
        contract_address,
        `to`
    FROM {{ source('sushiswap_v2_avalanche_c', 'SushiSwapRouter_call_swapETHForExactTokens') }}
    WHERE call_success = true
    {% if is_incremental() %}
    AND call_block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}

    UNION ALL

    SELECT
        call_block_number,
        call_block_time,
        call_trace_address,
        call_tx_hash,
        contract_address,
        `to`
    FROM {{ source('sushiswap_v2_avalanche_c', 'SushiSwapRouter_call_swapExactETHForTokens') }}
    WHERE call_success = true
    {% if is_incremental() %}
    AND call_block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}

    UNION ALL

    SELECT
        call_block_number,
        call_block_time,
        call_trace_address,
        call_tx_hash,
        contract_address,
        `to`
    FROM {{ source('sushiswap_v2_avalanche_c', 'SushiSwapRouter_call_swapExactTokensForETH') }}
    WHERE call_success = true
    {% if is_incremental() %}
    AND call_block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}

    UNION ALL

    SELECT
        call_block_number,
        call_block_time,
        call_trace_address,
        call_tx_hash,
        contract_address,
        `to`
    FROM {{ source('sushiswap_v2_avalanche_c', 'SushiSwapRouter_call_swapExactTokensForTokens') }}
    WHERE call_success = true
    {% if is_incremental() %}
    AND call_block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}

    UNION ALL

    SELECT
        call_block_number,
        call_block_time,
        call_trace_address,
        call_tx_hash,
        contract_address,
        `to`
    FROM {{ source('sushiswap_v2_avalanche_c', 'SushiSwapRouter_call_swapTokensForExactETH') }}
    WHERE call_success = true
    {% if is_incremental() %}
    AND call_block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}

    UNION ALL

    SELECT
        call_block_number,
        call_block_time,
        call_trace_address,
        call_tx_hash,
        contract_address,
        `to`
    FROM {{ source('sushiswap_v2_avalanche_c', 'SushiSwapRouter_call_swapTokensForExactTokens') }}
    WHERE call_success = true
    {% if is_incremental() %}
    AND call_block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}

    UNION ALL

    SELECT
        call_block_number,
        call_block_time,
        call_trace_address,
        call_tx_hash,
        contract_address,
        `to`
    FROM {{ source('sushiswap_v2_avalanche_c', 'SushiSwapRouter_call_swapExactETHForTokensSupportingFeeOnTransferTokens') }}
    WHERE call_success = true
    {% if is_incremental() %}
    AND call_block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}

    UNION ALL

    SELECT
        call_block_number,
        call_block_time,
        call_trace_address,
        call_tx_hash,
        contract_address,
        `to`
    FROM {{ source('sushiswap_v2_avalanche_c', 'SushiSwapRouter_call_swapExactTokensForETHSupportingFeeOnTransferTokens') }}
    WHERE call_success = true
    {% if is_incremental() %}
    AND call_block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}

    UNION ALL

    SELECT
        call_block_number,
        call_block_time,
        call_trace_address,
        call_tx_hash,
        contract_address,
        `to`
    FROM {{ source('sushiswap_v2_avalanche_c', 'SushiSwapRouter_call_swapExactTokensForTokensSupportingFeeOnTransferTokens') }}
    WHERE call_success = true
    {% if is_incremental() %}
    AND call_block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}
),

sushiswap_decodes_with_log AS (
    SELECT
        bytea2numeric_v2(substr(l.data, 3, 64))                    AS amount0In,
        bytea2numeric_v2(substr(l.data, 3 + 64, 64))               AS amount1In,
        bytea2numeric_v2(substr(l.data, 3 + 64 + 64, 64))          AS amount0Out,
        bytea2numeric_v2(substr(l.data, 3 + 64 + 64 + 64 + 1, 64)) AS amount1Out,
        l.index                                                    AS evt_index,
        p.token0                                                   AS token0,
        p.token1                                                   AS token1,
        call_block_number,
        call_block_time,
        call_trace_address,
        call_tx_hash,
        t.contract_address,
        call_trace_address,
        `to`
    FROM sushiswap_decodes t
    INNER JOIN {{ source('avalanche_c', 'logs') }} l
        ON t.call_tx_hash = l.tx_hash
        AND l.topic1 = "0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822"
        {% if is_incremental() %}
        AND l.block_time >= date_trunc("day", now() - interval '1 week')
        {% else %}
        AND l.block_time >= '{{ project_start_date }}'
        {% endif %}
    INNER JOIN {{ source('sushiswap_v2_avalanche_c', 'SushiV2Factory_evt_PairCreated') }} p
        ON l.contract_address = p.pair
),

sushiswap_dex AS (
    SELECT  call_block_time                                              AS block_time,
            `to`                                                         AS taker,
            ''                                                           AS maker,
            CASE WHEN amount0Out = 0 THEN amount1Out ELSE amount0Out END AS token_bought_amount_raw,
            CASE WHEN amount0In = 0 THEN amount1In ELSE amount0In END    AS token_sold_amount_raw,
            cast(NULL as double)                                         AS amount_usd,
            CASE WHEN amount0Out = 0 THEN token1 ELSE token0 END         AS token_bought_address,
            CASE WHEN amount0In = 0 THEN token1 ELSE token0 END          AS token_sold_address,
            contract_address                                             AS project_contract_address,
            call_tx_hash                                                 AS tx_hash,
            call_trace_address                                           AS trace_address,
            evt_index
    FROM sushiswap_decodes_with_log
)

SELECT
    'avalanche_c'                                                      AS blockchain,
    'sushiswap'                                                        AS project,
    '2'                                                                AS version,
    try_cast(date_trunc('DAY', sushiswap_dex.block_time) AS date)      AS block_date,
    sushiswap_dex.block_time,
    erc20a.symbol                                                      AS token_bought_symbol,
    erc20b.symbol                                                      AS token_sold_symbol,
    case
        when lower(erc20a.symbol) > lower(erc20b.symbol) then concat(erc20b.symbol, '-', erc20a.symbol)
        else concat(erc20a.symbol, '-', erc20b.symbol)
        end                                                            AS token_pair,
    sushiswap_dex.token_bought_amount_raw / power(10, erc20a.decimals) AS token_bought_amount,
    sushiswap_dex.token_sold_amount_raw / power(10, erc20b.decimals)   AS token_sold_amount,
    sushiswap_dex.token_bought_amount_raw,
    sushiswap_dex.token_sold_amount_raw,
    coalesce(
            sushiswap_dex.amount_usd
        , (sushiswap_dex.token_bought_amount_raw / power(10, p_bought.decimals)) * p_bought.price
        , (sushiswap_dex.token_sold_amount_raw / power(10, p_sold.decimals)) * p_sold.price
        )                                                              AS amount_usd,
    sushiswap_dex.token_bought_address,
    sushiswap_dex.token_sold_address,
    coalesce(sushiswap_dex.taker, tx.from)                             AS taker,
    sushiswap_dex.maker,
    sushiswap_dex.project_contract_address,
    sushiswap_dex.tx_hash,
    tx.from                                                            AS tx_from,
    tx.to                                                              AS tx_to,
    sushiswap_dex.trace_address,
    sushiswap_dex.evt_index
from sushiswap_dex
inner join {{ source('avalanche_c', 'transactions') }} tx
    on sushiswap_dex.tx_hash = tx.hash
    {% if is_incremental() %}
    and tx.block_time >= date_trunc("day", now() - interval '1 week')
    {% else %}
    and tx.block_time >= '{{project_start_date}}'
    {% endif %}
left join {{ ref('tokens_erc20') }} erc20a
    on erc20a.contract_address = sushiswap_dex.token_bought_address
    and erc20a.blockchain = 'avalanche_c'
left join {{ ref('tokens_erc20') }} erc20b
    on erc20b.contract_address = sushiswap_dex.token_sold_address
    and erc20b.blockchain = 'avalanche_c'
left join {{ source('prices', 'usd') }} p_bought
    on p_bought.minute = date_trunc('minute', sushiswap_dex.block_time)
    and p_bought.contract_address = sushiswap_dex.token_bought_address
    and p_bought.blockchain = 'avalanche_c'
    {% if is_incremental() %}
    and p_bought.minute >= date_trunc("day", now() - interval '1 week')
    {% else %}
    and p_bought.minute >= '{{project_start_date}}'
    {% endif %}
left join {{ source('prices', 'usd') }} p_sold
    on p_sold.minute = date_trunc('minute', sushiswap_dex.block_time)
    and p_sold.contract_address = sushiswap_dex.token_sold_address
    and p_sold.blockchain = 'avalanche_c'
    {% if is_incremental() %}
    and p_sold.minute >= date_trunc("day", now() - interval '1 week')
    {% else %}
    and p_sold.minute >= '{{project_start_date}}'
    {% endif %}
;