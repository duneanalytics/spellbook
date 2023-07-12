{{ config(
    alias = 'trades',
    partition_by = ['block_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'blockchain', 'project', 'version', 'tx_hash', 'evt_index', 'trace_address'],
    )
}}

{% set project_start_date = '2017-09-27 00:00:00' %}

--see curvefi readme for more info on the logic below.
WITH dexs AS
(
    --factory v1 and regular pools
    SELECT
        l.block_time
        , p.version as version
        , '0x' || substring(l.topic2, 27,40) as taker
        , '' as maker
        , case
            when l.topic1 = "0xd013ca23e77a65003c2c659c5442c00c805371b7fc1ebd4c206c41d1536bd90b"
                        AND cast(substring(l.data, 131, 64) as int) = 0
                then 'underlying_exchange_base'
                else 'normal_exchange'
            end as swap_type
        , bytea2numeric(substring(l.data, 195, 64)) as token_bought_amount_raw
        , bytea2numeric(substring(l.data, 67, 64)) as token_sold_amount_raw
        , cast(NULL as double) AS amount_usd
        , case
            when l.topic1 = "0x8b3e96f2b889fa771c53c981b40daf005f63f637f1869f707052d15a3dd97140"
                then p.coins[cast(substring(l.data, 131, 64) as int)] 
                else p.undercoins[cast(substring(l.data, 131, 64) as int)]
            end as token_bought_address
        , case
            when l.topic1 = "0x8b3e96f2b889fa771c53c981b40daf005f63f637f1869f707052d15a3dd97140"
                then p.coins[cast(substring(l.data, 3, 64) as int)] 
                else p.undercoins[cast(substring(l.data, 3, 64) as int)]
            end as token_sold_address
        , l.contract_address as project_contract_address --pool address
        , l.tx_hash 
        , '' as trace_address
        , l.index as evt_index
    FROM {{ source('ethereum', 'logs') }} l
    JOIN  {{ ref('curvefi_ethereum_view_pools') }} p
        ON l.contract_address = p.pool_address
        AND p.version IN ('Factory V1 Meta', 'Factory V1 Plain', 'Regular') --note Plain only has TokenExchange.
    WHERE l.topic1 IN
        (
            "0xd013ca23e77a65003c2c659c5442c00c805371b7fc1ebd4c206c41d1536bd90b" -- TokenExchangeUnderlying 
            , "0x8b3e96f2b889fa771c53c981b40daf005f63f637f1869f707052d15a3dd97140" -- TokenExchange
        )
        {% if not is_incremental() %}
        AND l.block_time >= '{{project_start_date}}'
        {% endif %}
        {% if is_incremental() %}
        AND l.block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}

    UNION ALL

    --factory v2 pools and v1 plain pools have same logic
    SELECT
        l.block_time
        , p.version as version
        , '0x' || substring(l.topic2, 27,40) as taker
        , '' as maker
        , 'normal_exchange' as swap_type
        , bytea2numeric(substring(l.data, 195, 64)) as token_bought_amount_raw
        , bytea2numeric(substring(l.data, 67, 64)) as token_sold_amount_raw
        , cast(NULL as double) AS amount_usd
        , p.coins[cast(substring(l.data, 131, 64) as int)] as token_bought_address
        , p.coins[cast(substring(l.data, 3, 64) as int)] as token_sold_address
        , l.contract_address as project_contract_address --pool address
        , l.tx_hash 
        , '' as trace_address
        , l.index as evt_index
    FROM {{ source('ethereum', 'logs') }} l
    JOIN  {{ ref('curvefi_ethereum_view_pools') }} p
        ON l.contract_address = p.pool_address
        AND (p.version = 'Factory V2' or p.version = 'Regular') --some Regular pools are new and use the below topic instead
    WHERE l.topic1 = "0xb2e76ae99761dc136e598d4a629bb347eccb9532a5f8bbd72e18467c3c34cc98" --TokenExchange
        {% if not is_incremental() %}
        AND l.block_time >= '{{project_start_date}}'
        {% endif %}
        {% if is_incremental() %}
        and l.block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
)

SELECT
    'ethereum' AS blockchain
    ,'curve' AS project
    ,dexs.version AS version
    ,TRY_CAST(date_trunc('DAY', dexs.block_time) AS date) AS block_date
    ,dexs.block_time
    ,erc20a.symbol AS token_bought_symbol
    ,erc20b.symbol AS token_sold_symbol
    ,case
        when lower(erc20a.symbol) > lower(erc20b.symbol) then concat(erc20b.symbol, '-', erc20a.symbol)
        else concat(erc20a.symbol, '-', erc20b.symbol)
    end as token_pair
    ,dexs.token_bought_amount_raw / power(10, erc20a.decimals) AS token_bought_amount
    ,dexs.token_sold_amount_raw / power(10, case when swap_type = 'underlying_exchange_base' then 18 else erc20b.decimals end) AS token_sold_amount
    ,CAST(dexs.token_bought_amount_raw AS DECIMAL(38,0)) AS token_bought_amount_raw
    ,CAST(dexs.token_sold_amount_raw AS DECIMAL(38,0)) AS token_sold_amount_raw
    ,coalesce(
        dexs.amount_usd
        ,(dexs.token_bought_amount_raw / power(10, p_bought.decimals)) * p_bought.price
        ,(dexs.token_sold_amount_raw / power(10, case when swap_type = 'underlying_exchange_base' then 18 else p_sold.decimals end)) * p_sold.price
    ) AS amount_usd
    ,dexs.token_bought_address
    ,dexs.token_sold_address
    ,coalesce(dexs.taker, tx.from) AS taker -- subqueries rely on this COALESCE to avoid redundant joins with the transactions table
    ,dexs.maker
    ,dexs.project_contract_address
    ,dexs.tx_hash
    ,tx.from AS tx_from
    ,tx.to AS tx_to
    ,dexs.trace_address
    ,dexs.evt_index
FROM dexs
INNER JOIN {{ source('ethereum', 'transactions') }} tx
    ON tx.hash = dexs.tx_hash
    {% if not is_incremental() %}
    -- The date below is derrived from `select min(evt_block_time) from uniswap_ethereum.Factory_evt_NewExchange;`
    -- If dexs above is changed then this will also need to be changed.
    AND tx.block_time >= '{{project_start_date}}'
    {% endif %}
    {% if is_incremental() %}
    AND tx.block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}
LEFT JOIN {{ ref('tokens_ethereum_erc20_legacy') }} erc20a ON erc20a.contract_address = dexs.token_bought_address
LEFT JOIN {{ ref('tokens_ethereum_erc20_legacy') }} erc20b ON erc20b.contract_address = dexs.token_sold_address
LEFT JOIN {{ source('prices', 'usd') }} p_bought ON p_bought.minute = date_trunc('minute', dexs.block_time)
    AND p_bought.contract_address = dexs.token_bought_address
    AND p_bought.blockchain = 'ethereum'
    {% if not is_incremental() %}
    -- The date below is derrived from `select min(evt_block_time) from uniswap_ethereum.Factory_evt_NewExchange;`
    -- If dexs above is changed then this will also need to be changed.
    AND p_bought.minute >= '{{project_start_date}}'
    {% endif %}
    {% if is_incremental() %}
    AND p_bought.minute >= date_trunc("day", now() - interval '1 week')
    {% endif %}
LEFT JOIN {{ source('prices', 'usd') }} p_sold ON p_sold.minute = date_trunc('minute', dexs.block_time)
    AND p_sold.contract_address = dexs.token_sold_address
    AND p_sold.blockchain = 'ethereum'
    {% if not is_incremental() %}
    -- The date below is derrived from `select min(evt_block_time) from uniswap_ethereum.Factory_evt_NewExchange;`
    -- If dexs above is changed then this will also need to be changed.
    AND p_sold.minute >= '{{project_start_date}}'
    {% endif %}
    {% if is_incremental() %}
    AND p_sold.minute >= date_trunc("day", now() - interval '1 week')
    {% endif %}
