/* 
    In order to compute the aggregated volume traded through the liquidity owned by a given vault,
    first we need to compute:
     - All the arrakis vaults created by the factory            (arrakis_vaults)
     - The liquidity of each vault at every swap                (liquidity_ts)
     - The price of each pool at every swap                     (pool_price_ts)
     - The price of each pool in USD at every swap              (pool_price_usd_ts)
     - The volume traded through each vault at every swap       (vault_swaps)
*/

{% macro arrakis_compatible_v2_trades(
    blockchain = null
    , project = null
    , version = null
    , dex = null
    , dex_version = null
    , Pair_evt_Mint = null
    , Pair_evt_Burn = null
    , Pair_evt_Swap = null
    , Factory_evt_PoolCreated = null
    , ArrakisV2Factory_evt_VaultCreated = null
    )
%}
 
------------------------------------------------------------------------------------------
-- GET POOL LIQUIDITY --------------------------------------------------------------------
------------------------------------------------------------------------------------------

WITH arrakis_vaults AS
(
    SELECT DISTINCT a.evt_block_time as creation_time
        , a.vault as vault_address
        , f.token0 AS token0_address
        , f.token1 AS token1_address
        , m.contract_address AS pool_address
    FROM
        {{ Pair_evt_Mint }} as m
    INNER JOIN
        {{ ArrakisV2Factory_evt_VaultCreated }} as a
        ON a.evt_block_time <= m.evt_block_time AND a.vault = m.owner
    INNER JOIN
        {{ Factory_evt_PoolCreated }} as f
        ON f.evt_block_time <= m.evt_block_time AND f.pool = m.contract_address
)

, mints AS
(
    SELECT m.evt_block_number AS block_number
        , m.evt_block_time AS block_time
        , m.evt_tx_hash AS tx_hash
        , m.evt_index
        , m.owner AS vault_address
        , m.tickLower AS tick_lower
        , m.tickUpper AS tick_upper
        , m.amount AS liquidity
        , m.contract_address AS pool_address
    FROM
        {{ Pair_evt_Mint }} as m
    INNER JOIN
        arrakis_vaults as a
        ON a.creation_time <= m.evt_block_time AND a.pool_address = m.contract_address AND a.vault_address = m.owner
    {% if is_incremental() %}
    WHERE
        {{ incremental_predicate('m.evt_block_time') }}
    {% endif %}
)

, burns AS
(
    SELECT b.evt_block_number AS block_number
        , b.evt_block_time AS block_time
        , b.evt_tx_hash AS tx_hash
        , b.evt_index
        , b.owner AS vault_address
        , b.tickLower AS tick_lower
        , b.tickUpper AS tick_upper
        , -b.amount AS liquidity
        , b.contract_address AS pool_address
    FROM
        {{ Pair_evt_Burn }} as b
    INNER JOIN
        arrakis_vaults as a
        ON a.creation_time <= b.evt_block_time AND a.pool_address = b.contract_address AND a.vault_address = b.owner
    {% if is_incremental() %}
    WHERE
        {{ incremental_predicate('b.evt_block_time') }}
    {% endif %}
)

, lp_data AS (
    SELECT block_number
        , block_time
        , evt_index
        , pool_address
        , vault_address
        , tick_lower
        , tick_upper
        , SUM(liquidity) OVER (PARTITION BY tick_lower, tick_upper, pool_address, vault_address ORDER BY block_number ASC, evt_index ASC) AS liquidity
    FROM (
        SELECT block_number
            , block_time
            , evt_index
            , pool_address
            , vault_address
            , tick_lower
            , tick_upper
            , SUM(liquidity) AS liquidity
        FROM (
            SELECT * FROM mints
            UNION ALL
            SELECT * FROM burns
        ) mints_and_burns
        GROUP BY 1,2,3,4,5,6,7
    ) lp_data
)

, time_series AS (
    -- all liquidity events
    SELECT DISTINCT block_time, evt_index, pool_address
    FROM lp_data
    UNION DISTINCT
    -- all swap events
    SELECT DISTINCT s.evt_block_time AS block_time, s.evt_index, a.pool_address
    FROM {{ Pair_evt_Swap }} as s
    INNER JOIN arrakis_vaults as a
        ON a.creation_time <= s.evt_block_time AND a.pool_address = s.contract_address
    {% if is_incremental() %}
    WHERE
        {{ incremental_predicate('s.evt_block_time') }}
    {% endif %}
)

, liquidity_ts AS (
    SELECT * FROM (
        SELECT DISTINCT block_time
            , evt_index
            , pool_address
            , vault_address
            , tick_lower
            , tick_upper
            , POWER(1.0001, tick_lower) AS pa
            , POWER(1.0001, tick_upper) AS pb
            , MIN(liquidity) OVER (PARTITION BY pool_address, tick_lower, tick_upper, adj_index ORDER BY block_time ASC, evt_index ASC) AS liquidity
        FROM (
            SELECT *, SUM(index_check) OVER (PARTITION BY pool_address, tick_lower, tick_upper ORDER BY block_time ASC, evt_index ASC) AS adj_index
            FROM (
                SELECT ts.*
                    , l.liquidity
                    , CASE WHEN l.liquidity IS NOT NULL THEN 1 END AS index_check
                FROM (
                    SELECT DISTINCT ts.*, l.vault_address, l.tick_lower, l.tick_upper
                    FROM time_series AS ts
                    INNER JOIN lp_data AS l
                        ON ts.pool_address = l.pool_address
                ) AS ts
                LEFT JOIN lp_data AS l
                    ON ts.block_time = l.block_time AND ts.evt_index = l.evt_index AND
                       ts.pool_address = l.pool_address AND ts.vault_address = l.vault_address AND
                       ts.tick_lower = l.tick_lower AND ts.tick_upper = l.tick_upper
            ) AS l
        ) AS l_adj
    ) WHERE liquidity is not null AND liquidity > 0
)

------------------------------------------------------------------------------------------
-- GET POOL PRICE ------------------------------------------------------------------------
------------------------------------------------------------------------------------------

, pool_price_ts AS (
    SELECT p.*
        , a.token0_address
        , a.token1_address
        , '{{ blockchain }}' AS blockchain
    FROM (
        SELECT DISTINCT block_time
            , pool_address
            , MIN(price) OVER (PARTITION BY pool_address, adj_index ORDER BY block_time) AS price
        FROM (
            SELECT *, SUM(index_check) OVER (PARTITION BY pool_address ORDER BY block_time) AS adj_index
            FROM (
                SELECT ts.block_time
                    , ts.pool_address
                    , AVG(POWER(1.0001, s.tick)) AS price
                    , CASE WHEN s.tick IS NOT NULL THEN 1 END AS index_check
                FROM time_series AS ts
                LEFT JOIN {{ Pair_evt_Swap }} AS s
                    ON ts.pool_address = s.contract_address AND ts.block_time = s.evt_block_time
                GROUP BY 1,2,4
            ) AS p
        ) AS p_adj
    ) AS p
    INNER JOIN arrakis_vaults AS a
        ON a.pool_address = p.pool_address
    WHERE price IS NOT NULL
)

, pool_price_usd_ts AS (
    {{
        add_pool_price_usd(
            pool_prices_cte = 'pool_price_ts'
        )
    }}
)

------------------------------------------------------------------------------------------
-- GET VOLUME PROVIDED BY EACH VAULT -----------------------------------------------------
------------------------------------------------------------------------------------------

,vault_swaps as (
    select distinct s.block_time
        , s.block_number
        , s.tx_hash
        , s.evt_index
        , s.pool_address
        , s.vault_address
        , greatest(least(s.sqrt_price, sqrt(lp.pb)), sqrt(lp.pa)) as sqrt_price
        , greatest(least(s.prev_sqrt_price, sqrt(lp.pb)), sqrt(lp.pa)) as prev_sqrt_price
        , lp.liquidity
        , s.swap_volume0
        , s.swap_volume1
    from liquidity_ts as lp
    inner join (
        SELECT s.evt_block_time as block_time
            , s.evt_block_number as block_number
            , s.evt_tx_hash as tx_hash
            , s.evt_index
            , a.pool_address
            , a.vault_address
            , lag(cast(s.sqrtPriceX96 as double), 1) over (partition by a.pool_address order by s.evt_block_number asc, s.evt_index asc) / pow(2,96) as prev_sqrt_price
            , cast(s.sqrtPriceX96 as double) / pow(2,96) as sqrt_price
            , abs(cast(s.amount0 as double)) as swap_volume0
            , abs(cast(s.amount1 as double)) as swap_volume1
        FROM {{ Pair_evt_Swap }} AS s
        INNER JOIN arrakis_vaults AS a
            ON a.creation_time <= s.evt_block_time AND a.pool_address = s.contract_address
        {% if is_incremental() %}
        WHERE
            {{ incremental_predicate('s.evt_block_time') }}
        {% endif %}
    ) AS s ON s.block_time = lp.block_time AND s.evt_index = lp.evt_index AND s.pool_address = lp.pool_address AND s.vault_address = lp.vault_address
    -- overlapping ranges only
    where sqrt(lp.pa) <= (case when s.sqrt_price > s.prev_sqrt_price then s.sqrt_price else s.prev_sqrt_price end)
        and (case when s.prev_sqrt_price < s.sqrt_price then s.prev_sqrt_price else s.sqrt_price end) <= sqrt(lp.pb)
)

select distinct '{{ blockchain }}' AS blockchain
    , '{{ project }}' AS project
    , '{{ version }}' AS version
    , '{{ dex }}' AS dex
    , '{{ dex_version }}' AS dex_version
    , v.block_time
    , date_trunc('MONTH',v.block_time) AS block_month
    , v.block_number
    , v.tx_hash
    , v.evt_index
    , a.pool_address
    , a.vault_address
    , concat(t0.symbol, '-', t1.symbol) as token_pair
    , a.token0_address
    , a.token1_address
    , v.volume1 / p.price AS volume0_raw
    , v.volume1 / (p.price * power(10, coalesce(t0.decimals, 18))) AS volume0
    , v.swap_volume0 / power(10, coalesce(t0.decimals, 18)) AS swap_volume0
    , t0.symbol AS token0_symbol    
    , v.volume1 AS volume1_raw
    , v.volume1 / power(10, coalesce(t1.decimals, 18)) AS volume1
    , v.swap_volume1 / power(10, coalesce(t1.decimals, 18)) AS swap_volume1
    , t1.symbol AS token1_symbol
    , p.price
    , p.price_usd
    , v.volume1 / (p.price * power(10, coalesce(t0.decimals, 18))) * p.price_usd AS volume_usd
    , v.swap_volume1 / (p.price * power(10, coalesce(t0.decimals, 18))) * p.price_usd AS swap_volume_usd
    , v.volume1 / v.swap_volume1 AS volume_share
from (
    select block_time
        , block_number
        , tx_hash
        , evt_index
        , pool_address
        , vault_address
        , sum(liquidity * abs(prev_sqrt_price - sqrt_price)) as volume1
        , sum(swap_volume0) as swap_volume0
        , sum(swap_volume1) as swap_volume1
    from vault_swaps
    group by 1,2,3,4,5,6
) as v
inner join pool_price_usd_ts as p on p.block_time = v.block_time and p.pool_address = v.pool_address
inner join arrakis_vaults as a on a.pool_address = v.pool_address and a.vault_address = v.vault_address
    and a.token0_address = p.token0_address and a.token1_address = p.token1_address
left join {{ source('tokens', 'erc20') }} as t0 on t0.contract_address = a.token0_address and t0.blockchain = '{{ blockchain }}'
left join {{ source('tokens', 'erc20') }} as t1 on t1.contract_address = a.token1_address and t1.blockchain = '{{ blockchain }}'
{% endmacro %}