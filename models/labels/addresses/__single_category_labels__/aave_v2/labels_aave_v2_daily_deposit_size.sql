{{config(alias = alias('aave_v2_deposit_size'))}}

with latest_net_deposits AS ( 
        with annual_flows AS (  
            SELECT 
                d.evt_block_time
                , d.user
                , cast(d.amount as double) / pow(10, p.decimals) AS amount
                , d.reserve
                , p.symbol
                , d.contract_address AS lendingpool_contract
            FROM {{ source('aave_v2_ethereum', 'LendingPool_evt_Deposit')}} d 
            LEFT JOIN {{ ref('prices_tokens') }} p
                ON p.contract_address = d.reserve
                AND p.blockchain = 'ethereum'
            WHERE evt_block_time > NOW() - INTERVAL '1' day
            
            UNION ALL
            
            SELECT 
                w.evt_block_time
                , w.user
                , cast(w.amount as double) * -1 / pow(10, p.decimals) AS amount
                , w.reserve
                , p.symbol
                , w.contract_address AS lendingpool_contract
            FROM {{ source('aave_v2_ethereum', 'LendingPool_evt_Withdraw')}} w
            LEFT JOIN {{ ref('prices_tokens') }} p
                ON p.contract_address = w.reserve
                AND p.blockchain = 'ethereum'
            WHERE evt_block_time > NOW() - INTERVAL '1' day
        )
        
        , annual_net_deposit AS (
        SELECT 
            user
            , SUM(amount) AS net_deposit
            , reserve
            , symbol
            , evt_block_time
        FROM annual_flows
        WHERE amount > 0
        GROUP BY 1,3,4,5
        ORDER BY net_deposit DESC
        )
        
        , ordered_annual_net_deposit AS (
        SELECT
            row_number() OVER (partition by a.user, a.reserve ORDER BY a.evt_block_time DESC) AS rn
            , a.user
            , a.net_deposit
            , a.reserve
            , a.symbol
            , a.evt_block_time
        FROM annual_net_deposit a
        )
        
        SELECT
            user
            , net_deposit
            , reserve
            , symbol
            , evt_block_time
        FROM ordered_annual_net_deposit 
        WHERE rn = 1
        ORDER BY net_deposit DESC
)

, latest_available_liquidity AS (
        with cte as (
            SELECT
                row_number() OVER (partition by d.reserve, p.symbol ORDER BY d.call_block_time DESC) AS rn
                , d.reserve
                , p.symbol
                , cast(d.availableLiquidity as double) / pow(10, p.decimals) AS available_liquidity
                , d.call_block_time
            FROM {{ source('aave_v2_ethereum', 'DefaultReserveInterestRateStrategy_call_calculateInterestRates')}} d
            LEFT JOIN {{ ref('prices_tokens') }} p
                ON p.contract_address = d.reserve
                AND p.blockchain = 'ethereum'
            ORDER BY call_block_time DESC
        )
        
        SELECT
            reserve
            , symbol
            , available_liquidity
            , call_block_time
        FROM cte
        WHERE rn = 1
        ORDER BY call_block_time ASC
)

, calc_deposit_pct AS (
SELECT
    d.user
    , d.net_deposit
    , l.available_liquidity
    , d.net_deposit / l.available_liquidity * 100 AS largest_deposit_pct
    , d.reserve
    , d.symbol
    , d.evt_block_time
FROM latest_net_deposits d
LEFT JOIN latest_available_liquidity l ON d.reserve = l.reserve AND d.symbol = l.symbol
)

, final_base_label AS (
SELECT
    'ethereum' AS blockchain
    , user AS address 
    , case when COALESCE(largest_deposit_pct, 0) <= 1 then 'Small Depositor'
        when largest_deposit_pct <= 10 then 'Sizeable Depositor'
        else 'Large Depositor'
        end as name
    , 'deposit size' AS category
    , 'paulx' AS contributor
    , 'wizard' AS source
    , date('2023-03-26') AS created_at
    , now() AS updated_at
    , 'aave_v2 daily deposits' AS model_name
    , 'persona' AS label_type
    , net_deposit
    , available_liquidity
    , largest_deposit_pct
    , symbol 
FROM calc_deposit_pct
)

SELECT
    blockchain
    , address
    , name
    , category
    , contributor
    , source
    , created_at
    , updated_at
    , model_name
    , label_type
FROM final_base_label
;