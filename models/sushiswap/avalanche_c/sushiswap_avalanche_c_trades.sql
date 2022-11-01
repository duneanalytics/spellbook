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

{% set project_start_date = '2021-03-15' %}

WITH sushiswap_dex AS (
    SELECT  t.evt_block_time                                             AS block_time,
            `to`                                                         AS taker,
            sender                                                       AS maker,
            CASE WHEN amount0Out = 0 THEN amount1Out ELSE amount0Out END AS token_bought_amount_raw,
            CASE WHEN amount0In = 0 THEN amount1In ELSE amount0In END    AS token_sold_amount_raw,
            cast(NULL as double)                                         AS amount_usd,
            CASE WHEN amount0Out = 0 THEN token1 ELSE token0 END         AS token_bought_address,
            CASE WHEN amount0In = 0 THEN token1 ELSE token0 END          AS token_sold_address,
            contract_address                                             AS project_contract_address,
            t.evt_tx_hash                                                AS tx_hash,
            ''                                                           AS trace_address,
            t.evt_index
    FROM {{ source('sushiswap_v2_avalanche_c', 'Pair_evt_Swap') }} t
    INNER JOIN {{ source('sushiswap_v2_avalanche_c', 'SushiV2Factory_evt_PairCreated') }} p
        ON t.contract_address = p.pair
        {% if is_incremental() %}
        AND t.evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% else %}
        AND t.evt_block_time >= '{{ project_start_date }}'
        {% endif %}
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