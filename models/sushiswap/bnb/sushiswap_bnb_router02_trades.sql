{{ config(
    alias = 'router02_trades'
    ,partition_by = ['block_date']
    ,materialized = 'incremental'
    ,file_format = 'delta'
    ,incremental_strategy = 'merge'
    ,unique_key = ['block_date', 'blockchain', 'project', 'version', 'tx_hash', 'evt_index', 'trace_address']
    ,post_hook='{{ expose_spells(\'["bnb"]\',
                                      "project",
                                      "sushiswap",
                                    \'["hosuke"]\') }}'
    )
}}

{% set project_start_date = '2021-03-03' %} -- fork from sushiswap_bnb_trades

{% set sushi_bnb_router02_swaps = [
    source('sushi_bnb', 'UniswapV2Router02_call_swapETHForExactTokens')
    , source('sushi_bnb', 'UniswapV2Router02_call_swapExactETHForTokens')
    , source('sushi_bnb', 'UniswapV2Router02_call_swapExactETHForTokensSupportingFeeOnTransferTokens')
    , source('sushi_bnb', 'UniswapV2Router02_call_swapExactTokensForETH')
    , source('sushi_bnb', 'UniswapV2Router02_call_swapExactTokensForETHSupportingFeeOnTransferTokens')
    , source('sushi_bnb', 'UniswapV2Router02_call_swapExactTokensForTokens')
    , source('sushi_bnb', 'UniswapV2Router02_call_swapExactTokensForTokensSupportingFeeOnTransferTokens')
    , source('sushi_bnb', 'UniswapV2Router02_call_swapTokensForExactETH')
    , source('sushi_bnb', 'UniswapV2Router02_call_swapTokensForExactTokens')
] %}

WITH sushiswap_decodes AS (
    {% for swaps_evt in sushi_bnb_router02_swaps %}
        SELECT
            call_block_time,
            call_trace_address,
            call_tx_hash,
            contract_address,
            s.to
        FROM {{ swaps_evt }} s
        WHERE call_success = true
        {% if is_incremental() %}
        AND call_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}

        {% if not loop.last %}
        UNION ALL
        {% endif %}

    {% endfor %}
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
        call_block_time,
        call_trace_address,
        call_tx_hash,
        t.contract_address,
        call_trace_address,
        t.to
    FROM sushiswap_decodes t
    INNER JOIN {{ source('bnb', 'logs') }} l
        ON t.call_tx_hash = l.tx_hash
        AND l.topic1 = "0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822"
        {% if is_incremental() %}
        AND l.block_time >= date_trunc("day", now() - interval '1 week')
        {% else %}
        AND l.block_time >= '{{ project_start_date }}'
        {% endif %}
    INNER JOIN {{ source('sushi_bnb', 'UniswapV2Factory_evt_PairCreated') }} p
        ON l.contract_address = p.pair
),

sushiswap_dex AS (
    SELECT  call_block_time                                              AS block_time,
            s.to                                                      AS taker,
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
    FROM sushiswap_decodes_with_log s
)

SELECT
    'bnb'                                                              AS blockchain,
    'sushiswap'                                                        AS project,
    '1'                                                                AS version,
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
inner join {{ source('bnb', 'transactions') }} tx
    on sushiswap_dex.tx_hash = tx.hash
    {% if is_incremental() %}
    and tx.block_time >= date_trunc("day", now() - interval '1 week')
    {% else %}
    and tx.block_time >= '{{project_start_date}}'
    {% endif %}
left join {{ ref('tokens_erc20') }} erc20a
    on erc20a.contract_address = sushiswap_dex.token_bought_address
    and erc20a.blockchain = 'bnb'
left join {{ ref('tokens_erc20') }} erc20b
    on erc20b.contract_address = sushiswap_dex.token_sold_address
    and erc20b.blockchain = 'bnb'
left join {{ source('prices', 'usd') }} p_bought
    on p_bought.minute = date_trunc('minute', sushiswap_dex.block_time)
    and p_bought.contract_address = sushiswap_dex.token_bought_address
    and p_bought.blockchain = 'bnb'
    {% if is_incremental() %}
    and p_bought.minute >= date_trunc("day", now() - interval '1 week')
    {% else %}
    and p_bought.minute >= '{{project_start_date}}'
    {% endif %}
left join {{ source('prices', 'usd') }} p_sold
    on p_sold.minute = date_trunc('minute', sushiswap_dex.block_time)
    and p_sold.contract_address = sushiswap_dex.token_sold_address
    and p_sold.blockchain = 'bnb'
    {% if is_incremental() %}
    and p_sold.minute >= date_trunc("day", now() - interval '1 week')
    {% else %}
    and p_sold.minute >= '{{project_start_date}}'
    {% endif %}
;