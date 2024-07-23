{{
    config(

        schema = 'balancer_cowswap_amm',
        alias = 'trades',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
        unique_key = ['tx_hash', 'evt_index'],
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                "project",
                                "balancer_cowswap_amm",
                                \'["viniabussafi"]\') }}'
    )
}}

    SELECT 
    'ethereum' AS blockchain
    , 'balancer_cowswap_amm' AS project
    , '1' AS version
    , CAST(date_trunc('month', settlement.evt_block_time) AS DATE) AS block_month
    , CAST(date_trunc('day', settlement.evt_block_time) AS DATE) AS block_date
    , settlement.evt_block_time AS block_time
    , settlement.evt_block_number block_number
    , tb.symbol AS token_bought_symbol
    , tb.symbol AS token_sold_symbol
    , CONCAT(ts.symbol, '-', tb.symbol) AS token_pair
    , (settlement.buyAmount / POWER(10, COALESCE(pb.decimals, tb.decimals))) AS token_bought_amount
    , (settlement.sellAmount / POWER(10, COALESCE(ps.decimals, ts.decimals))) AS token_sold_amount
    , settlement.buyAmount AS token_bought_amount_raw
    , settlement.sellAmount AS token_sold_amount_raw
    , COALESCE(settlement.buyAmount / POWER(10, COALESCE(pb.decimals, tb.decimals)) * pb.price,
    settlement.sellAmount / POWER(10, COALESCE(ps.decimals, ts.decimals)) * ps.price)
     AS amount_usd
    ,settlement.buyToken AS token_bought_address
    ,settlement.sellToken AS token_sold_address
    ,CAST(NULL AS VARBINARY) AS taker
    ,CAST(NULL AS VARBINARY) AS maker
    , pool.bPool AS project_contract_address
    , settlement.evt_tx_hash AS tx_hash
    , tx.from AS tx_from
    , tx.to AS tx_to
    , settlement.evt_index AS evt_index
    FROM {{ source('gnosis_protocol_v2_ethereum', 'GPv2Settlement_evt_Trade') }} settlement
    INNER JOIN {{ source('b_cow_amm_ethereum', 'BCoWFactory_evt_LOG_NEW_POOL') }} pool
        ON settlement.owner = pool.bPool
             LEFT JOIN {{ source('prices', 'usd') }} AS ps
                             ON sellToken = ps.contract_address
                                 AND ps.minute = date_trunc('minute', settlement.evt_block_time)
                                 AND ps.blockchain = 'ethereum'
                                 {% if is_incremental() %}
                                 AND {{ incremental_predicate('ps.minute') }}
                                 {% endif %}
             LEFT JOIN {{ source('prices', 'usd') }} AS pb
                             ON pb.contract_address = buyToken
                                 AND pb.minute = date_trunc('minute', settlement.evt_block_time)
                                 AND pb.blockchain = 'ethereum'
                                 {% if is_incremental() %}
                                 AND {{ incremental_predicate('pb.minute') }}
                                 {% endif %}
             LEFT JOIN {{ source('tokens', 'erc20') }} AS ts
                             ON settlement.sellToken = ts.contract_address
                                 AND ts.blockchain = 'ethereum'
             LEFT JOIN {{ source('tokens', 'erc20') }} AS tb
                             ON settlement.buyToken = tb.contract_address
                                 AND tb.blockchain = 'ethereum'   
             LEFT JOIN {{ source('ethereum', 'transactions') }} AS tx
                             ON settlement.evt_tx_hash = tb.hash
                                 AND settlement.evt_block_numbern = tx.block_number                                                                       
    {% if is_incremental() %}
    WHERE {{ incremental_predicate('settlement.evt_block_time') }}
    {% endif %}

