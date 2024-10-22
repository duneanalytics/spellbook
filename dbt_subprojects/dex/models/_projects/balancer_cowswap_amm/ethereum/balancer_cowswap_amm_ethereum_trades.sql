{% set blockchain = 'ethereum' %}

{{
    config(
        schema = 'balancer_cowswap_amm_' + blockchain,
        alias = 'trades',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
        unique_key = ['tx_hash', 'evt_index']
    )
}}

    SELECT 
    '{{blockchain}}' AS blockchain
    , 'balancer' AS project
    , '1' AS version
    , CAST(date_trunc('month', trade.evt_block_time) AS DATE) AS block_month
    , CAST(date_trunc('day', trade.evt_block_time) AS DATE) AS block_date
    , trade.evt_block_time AS block_time
    , trade.evt_block_number block_number
    , tb.symbol AS token_bought_symbol
    , ts.symbol AS token_sold_symbol 
    , CONCAT(ts.symbol, '-', tb.symbol) AS token_pair
    , (trade.buyAmount / POWER(10, COALESCE(pb.decimals, tb.decimals))) AS token_bought_amount
    , ((trade.sellAmount - trade.feeAmount) / POWER(10, COALESCE(ps.decimals, ts.decimals))) AS token_sold_amount
    , trade.buyAmount AS token_bought_amount_raw
    , trade.sellAmount AS token_sold_amount_raw
    , COALESCE(trade.buyAmount / POWER(10, COALESCE(pb.decimals, tb.decimals)) * pb.price,
    trade.sellAmount / POWER(10, COALESCE(ps.decimals, ts.decimals)) * ps.price)
     AS amount_usd
    ,trade.buyToken AS token_bought_address
    ,trade.sellToken AS token_sold_address
    ,CAST(NULL AS VARBINARY) AS taker
    ,CAST(NULL AS VARBINARY) AS maker
    , pool.bPool AS pool_id 
    , pool.bPool AS project_contract_address
    , trade.evt_tx_hash AS tx_hash
    , settlement.solver AS tx_from
    , trade.contract_address AS tx_to
    , trade.evt_index AS evt_index
    , p.name AS pool_symbol
    , p.pool_type
    , (trade.feeAmount / POWER (10, ts.decimals)) AS swap_fee
    FROM {{ source('gnosis_protocol_v2_ethereum', 'GPv2Settlement_evt_Trade') }} trade
    INNER JOIN {{ source('b_cow_amm_ethereum', 'BCoWFactory_evt_LOG_NEW_POOL') }} pool
        ON trade.owner = pool.bPool
    LEFT JOIN {{ source('prices', 'usd') }} AS ps
                    ON sellToken = ps.contract_address
                        AND ps.minute = date_trunc('minute', trade.evt_block_time)
                        AND ps.blockchain = '{{blockchain}}'
                        {% if is_incremental() %}
                        AND {{ incremental_predicate('ps.minute') }}
                        {% endif %}
    LEFT JOIN {{ source('prices', 'usd') }} AS pb
                    ON pb.contract_address = buyToken
                        AND pb.minute = date_trunc('minute', trade.evt_block_time)
                        AND pb.blockchain = '{{blockchain}}'
                        {% if is_incremental() %}
                        AND {{ incremental_predicate('pb.minute') }}
                        {% endif %}
    LEFT JOIN {{ source('tokens', 'erc20') }} AS ts
                    ON trade.sellToken = ts.contract_address
                        AND ts.blockchain = '{{blockchain}}'
    LEFT JOIN {{ source('tokens', 'erc20') }} AS tb
                    ON trade.buyToken = tb.contract_address
                        AND tb.blockchain = '{{blockchain}}'   
    LEFT JOIN {{ source('gnosis_protocol_v2_ethereum', 'GPv2Settlement_evt_Settlement') }} AS settlement
                    ON trade.evt_tx_hash = settlement.evt_tx_hash  
    LEFT JOIN {{ ref('labels_balancer_cowswap_amm_pools_ethereum') }} p ON p.address = trade.owner                                                                             
    {% if is_incremental() %}
    WHERE {{ incremental_predicate('trade.evt_block_time') }}
    {% endif %}
