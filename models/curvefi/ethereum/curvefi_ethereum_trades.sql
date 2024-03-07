{{ config(
	
    alias = 'trades',
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'blockchain', 'project', 'version', 'tx_hash', 'evt_index'],
    )
}}

{% set project_start_date = '2017-09-27' %}

--see curvefi readme for more info on the logic below.
WITH dexs AS
(
    --factory v1 and regular pools
    SELECT
        l.block_time
        , p.version as version
        , bytearray_substring(l.topic1, 13, 20) as taker
        , CAST(NULL as VARBINARY) as maker
        , case
            when l.topic0 = 0xd013ca23e77a65003c2c659c5442c00c805371b7fc1ebd4c206c41d1536bd90b
                        AND cast(bytearray_to_uint256(bytearray_substring(l.data, 65, 32)) as int) = 0
                then 'underlying_exchange_base'
                else 'normal_exchange'
            end as swap_type
        , bytearray_to_uint256(bytearray_substring(l.data, 97, 32)) as token_bought_amount_raw
        , bytearray_to_uint256(bytearray_substring(l.data, 33, 32)) as token_sold_amount_raw
        , cast(NULL as double) AS amount_usd
        , case
            when l.topic0 = 0x8b3e96f2b889fa771c53c981b40daf005f63f637f1869f707052d15a3dd97140
                then p.coins[cast(bytearray_to_uint256(bytearray_substring(l.data, 65, 32)) as int) + 1] 
                else p.undercoins[cast(bytearray_to_uint256(bytearray_substring(l.data, 65, 32)) as int) + 1]
            end as token_bought_address
        , case
            when l.topic0 = 0x8b3e96f2b889fa771c53c981b40daf005f63f637f1869f707052d15a3dd97140
                then p.coins[cast(bytearray_to_uint256(bytearray_substring(l.data, 1, 32)) as int) + 1] 
                else p.undercoins[cast(bytearray_to_uint256(bytearray_substring(l.data, 1, 32)) as int) + 1]
            end as token_sold_address
        , l.contract_address as project_contract_address --pool address
        , l.tx_hash 
        , l.index as evt_index
    FROM {{ source('ethereum', 'logs') }} l
    JOIN  {{ ref('curvefi_ethereum_view_pools') }} p
        ON l.contract_address = p.pool_address
        AND p.version IN ('Factory V1 Meta', 'Factory V1 Plain', 'Regular', 'Factory V1 Stableswap Plain', 'Factory V1 Stableswap Meta') --note Plain only has TokenExchange.
    WHERE l.topic0 IN
        (
            0xd013ca23e77a65003c2c659c5442c00c805371b7fc1ebd4c206c41d1536bd90b -- TokenExchangeUnderlying 
            , 0x8b3e96f2b889fa771c53c981b40daf005f63f637f1869f707052d15a3dd97140 -- TokenExchange
        )
        {% if not is_incremental() %}
        AND l.block_time >= TIMESTAMP '{{project_start_date}}'
        {% else %}
        AND l.block_time >= date_trunc('day', now() - interval '7' day)
        {% endif %}

    UNION ALL

    --factory v2 pools and v1 plain pools have same logic
    SELECT
        l.block_time
        , p.version as version
        , bytearray_substring(l.topic1, 13, 20) as taker
        , CAST(NULL as VARBINARY) as maker
        , 'normal_exchange' as swap_type
        , bytearray_to_uint256(bytearray_substring(l.data, 97, 32)) as token_bought_amount_raw
        , bytearray_to_uint256(bytearray_substring(l.data, 33, 32)) as token_sold_amount_raw
        , cast(NULL as double) AS amount_usd
        , p.coins[cast(bytearray_to_uint256(bytearray_substring(l.data, 65, 32)) as int) + 1] as token_bought_address
        , p.coins[cast(bytearray_to_uint256(bytearray_substring(l.data, 1, 32)) as int) + 1] as token_sold_address
        , l.contract_address as project_contract_address --pool address
        , l.tx_hash 
        , l.index as evt_index
    FROM {{ source('ethereum', 'logs') }} l
    JOIN {{ ref('curvefi_ethereum_view_pools') }} p
        ON l.contract_address = p.pool_address
        AND (p.version = 'Factory V2' or p.version = 'Factory V2 updated' or p.version = 'Regular') --some Regular pools are new and use the below topic instead
    WHERE l.topic0 = 0xb2e76ae99761dc136e598d4a629bb347eccb9532a5f8bbd72e18467c3c34cc98 --TokenExchange
        or l.topic0 =  0x143f1f8e861fbdeddd5b46e844b7d3ac7b86a122f36e8c463859ee6811b1f29c --TokenExchange (v2 updated pool, has some other variables included after old ones so topic hash is changed.)
        {% if not is_incremental() %}
        AND l.block_time >= TIMESTAMP '{{project_start_date}}'
        {% else %}
        and l.block_time >= date_trunc('day', now() - interval '7' day)
        {% endif %}
)

SELECT
    'ethereum' AS blockchain
    ,'curve' AS project
    ,dexs.version AS version
    ,CAST(date_trunc('DAY', dexs.block_time) AS date) AS block_date
    ,CAST(date_trunc('MONTH', dexs.block_time) AS date) AS block_month
    ,dexs.block_time
    ,erc20a.symbol AS token_bought_symbol
    ,erc20b.symbol AS token_sold_symbol
    ,case
        when lower(erc20a.symbol) > lower(erc20b.symbol) then concat(erc20b.symbol, '-', erc20a.symbol)
        else concat(erc20a.symbol, '-', erc20b.symbol)
    end as token_pair
    ,dexs.token_bought_amount_raw / power(10, erc20a.decimals) AS token_bought_amount
    ,dexs.token_sold_amount_raw / power(10, case when swap_type = 'underlying_exchange_base' then 18 else erc20b.decimals end) AS token_sold_amount
    ,dexs.token_bought_amount_raw  AS token_bought_amount_raw
    ,dexs.token_sold_amount_raw  AS token_sold_amount_raw
    ,coalesce(
        dexs.amount_usd
        ,(dexs.token_bought_amount_raw / power(10, p_bought.decimals)) * p_bought.price
        ,(dexs.token_sold_amount_raw / power(10, case when swap_type = 'underlying_exchange_base' then 18 else p_sold.decimals end)) * p_sold.price
    ) AS amount_usd
    ,dexs.token_bought_address
    ,dexs.token_sold_address
    ,coalesce(dexs.taker, tx."from") AS taker -- subqueries rely on this COALESCE to avoid redundant joins with the transactions table
    ,dexs.maker
    ,dexs.project_contract_address
    ,dexs.tx_hash
    ,tx."from" AS tx_from
    ,tx.to AS tx_to
    ,dexs.evt_index
FROM dexs
INNER JOIN {{ source('ethereum', 'transactions') }} tx
    ON tx.hash = dexs.tx_hash
    {% if not is_incremental() %}
    -- The date below is derrived from "select min(evt_block_time) from uniswap_ethereum.Factory_evt_NewExchange"
    -- If dexs above is changed then this will also need to be changed.
    AND tx.block_time >= TIMESTAMP '{{project_start_date}}'
    {% else %}
    AND tx.block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}
LEFT JOIN {{ source('tokens_ethereum', 'erc20') }} erc20a ON erc20a.contract_address = dexs.token_bought_address
LEFT JOIN {{ source('tokens_ethereum', 'erc20') }} erc20b ON erc20b.contract_address = dexs.token_sold_address
LEFT JOIN {{ source('prices', 'usd') }} p_bought ON p_bought.minute = date_trunc('minute', dexs.block_time)
    AND p_bought.contract_address = dexs.token_bought_address
    AND p_bought.blockchain = 'ethereum'
    {% if not is_incremental() %}
    -- The date below is derrived from "select min(evt_block_time) from uniswap_ethereum.Factory_evt_NewExchange"
    -- If dexs above is changed then this will also need to be changed.
    AND p_bought.minute >= TIMESTAMP '{{project_start_date}}'
    {% else %}
    AND p_bought.minute >= date_trunc('day', now() - interval '7' day)
    {% endif %}
LEFT JOIN {{ source('prices', 'usd') }} p_sold ON p_sold.minute = date_trunc('minute', dexs.block_time)
    AND p_sold.contract_address = dexs.token_sold_address
    AND p_sold.blockchain = 'ethereum'
    {% if not is_incremental() %}
    -- The date below is derrived from "select min(evt_block_time) from uniswap_ethereum.Factory_evt_NewExchange"
    -- If dexs above is changed then this will also need to be changed.
    AND p_sold.minute >= TIMESTAMP '{{project_start_date}}'
    {% else %}
    AND p_sold.minute >= date_trunc('day', now() - interval '7' day)
    {% endif %}
