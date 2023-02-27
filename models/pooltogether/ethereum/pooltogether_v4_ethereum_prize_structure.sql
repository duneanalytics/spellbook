CREATE OR REPLACE VIEW pooltogether_v4.tvl(

    -- total USDC in PoolTogether V4 on Optimism
with
    time_series as (
        select explode(sequence(to_date('{{Initial Date}}'), current_date(), interval 1 day))
        as days_ts
    ),
    OP_deposits as (
        select date_trunc('day', evt_block_time) AS day,
            SUM(value) / power(10, 6) AS deposit
        from erc20_optimism.evt_Transfer
        where contract_address = lower('0x625E7708f30cA75bfd92586e17077590C60eb4cD') --AAVE Optimism USDC
        and to = lower('0x4ecB5300D9ec6BCA09d66bfd8Dcb532e3192dDA1') --AaveV3YieldSource
        GROUP BY 1
        ORDER BY 1
    ),
    OP_withdrawals as (
        select date_trunc('day', evt_block_time) AS day,
            SUM(value) / power(10, 6) AS withdrawal
        from erc20_optimism.evt_Transfer
        where contract_address = lower('0x625E7708f30cA75bfd92586e17077590C60eb4cD') --AAVE Optimism USDC
        and from = lower('0x4ecB5300D9ec6BCA09d66bfd8Dcb532e3192dDA1') --AaveV3YieldSource
        GROUP BY 1
        ORDER BY 1
    ),
    OP_combo1 as (
        SELECT time_series.days_ts as days_ts, COALESCE(OP_deposits.deposit, 0) as deposit, COALESCE(OP_withdrawals.withdrawal, 0) as withdrawal
        FROM time_series
        left JOIN OP_deposits ON OP_deposits.day = time_series.days_ts
        left JOIN OP_withdrawals ON OP_withdrawals.day = time_series.days_ts
    ),
    OP_combo2 as (
    SELECT days_ts, SUM((deposit) - (withdrawal)) OVER (ORDER BY OP_combo1.days_ts) as sum1
    FROM OP_combo1
    )
    
-- total USDC in PoolTogether V4 on Ethereum
    ,ETH_deposits as (
        select date_trunc('day', evt_block_time) AS day,
            SUM(value) / power(10, 6) AS deposit
        from erc20_ethereum.evt_Transfer
        where contract_address = lower('0x32e8D4c9d1B711BC958d0Ce8D14b41F77Bb03a64') --ETH ATokenYieldSource
        and to = lower('0xd89a09084555a7D0ABe7B111b1f78DFEdDd638Be') --ETH YieldSourcePrizePool
        and from != lower ('0x42cd8312d2bce04277dd5161832460e95b24262e') -- Governance Contract
        GROUP BY 1
        ORDER BY 1
    ),
    ETH_withdrawals as (
        select date_trunc('day', evt_block_time) AS day,
            SUM(value) / power(10, 6) AS withdrawal
        from erc20_ethereum.evt_Transfer
        where contract_address = lower('0x32e8D4c9d1B711BC958d0Ce8D14b41F77Bb03a64') --ETH ATokenYieldSource
        and from = lower('0xd89a09084555a7D0ABe7B111b1f78DFEdDd638Be') --ETH YieldSourcePrizePool
        and to != lower ('0x42cd8312d2bce04277dd5161832460e95b24262e') -- Governance Contract
        GROUP BY 1
        ORDER BY 1
    ),
    ETH_combo1 as (
        SELECT time_series.days_ts as days_ts, COALESCE(ETH_deposits.deposit, 0) as deposit, COALESCE(ETH_withdrawals.withdrawal, 0) as withdrawal
        FROM time_series
        left JOIN ETH_deposits ON ETH_deposits.day = time_series.days_ts
        left JOIN ETH_withdrawals ON ETH_withdrawals.day = time_series.days_ts
    ),
    ETH_combo2 as (
    SELECT days_ts, SUM((deposit) - (withdrawal)) OVER (ORDER BY ETH_combo1.days_ts) as sum1
    FROM ETH_combo1
    )
    
-- total USDC in PoolTogether V4 on Avalanche
    ,AVAX_deposits as (
        select date_trunc('day', evt_block_time) AS day,
            SUM(value) / power(10, 6) AS deposit
        from erc20_avalanche_c.evt_Transfer
        where contract_address = lower('0x7437db21A0dEB844Fa64223e2d6Db569De9648Ff') --AVAX ATokenYieldSource
        and to = lower('0xF830F5Cb2422d555EC34178E27094a816c8F95EC') --AVAX YieldSourcePrizePool
        GROUP BY 1
        ORDER BY 1
    ),
    AVAX_withdrawals as (
        select date_trunc('day', evt_block_time) AS day,
            SUM(value) / power(10, 6) AS withdrawal
        from erc20_avalanche_c.evt_Transfer
        where contract_address = lower('0x7437db21A0dEB844Fa64223e2d6Db569De9648Ff') --AVAX ATokenYieldSource
        and from = lower('0xF830F5Cb2422d555EC34178E27094a816c8F95EC') --AVAX YieldSourcePrizePool
        GROUP BY 1
        ORDER BY 1
    ),
    AVAX_combo1 as (
        SELECT time_series.days_ts as days_ts, COALESCE(AVAX_deposits.deposit, 0) as deposit, COALESCE(AVAX_withdrawals.withdrawal, 0) as withdrawal
        FROM time_series
        left JOIN AVAX_deposits ON AVAX_deposits.day = time_series.days_ts
        left JOIN AVAX_withdrawals ON AVAX_withdrawals.day = time_series.days_ts
    ),
    AVAX_combo2 as (
    SELECT days_ts, SUM((deposit) - (withdrawal)) OVER (ORDER BY AVAX_combo1.days_ts) as sum1
    FROM AVAX_combo1
    )
 
-- total USDC in PoolTogether V4 on Polygon
,
    POLY_deposits as (
        select date_trunc('day', evt_block_time) AS day,
            SUM(value) / power(10, 6) AS deposit
        from erc20_polygon.evt_Transfer
        where contract_address = lower('0xD4F6d570133401079D213EcF4A14FA0B4bfB5b9C') and --POLY ATokenYieldSource
        to = lower('0x19de635fb3678d8b8154e37d8c9cdf182fe84e60') --POLY YieldSourcePrizePool
        GROUP BY 1
        ORDER BY 1
    ),
    POLY_withdrawals as (
        select date_trunc('day', evt_block_time) AS day,
            SUM(value) / power(10, 6) AS withdrawal
        from erc20_polygon.evt_Transfer
        where contract_address = lower('0xD4F6d570133401079D213EcF4A14FA0B4bfB5b9C') and --POLY ATokenYieldSource
        from = lower('0x19de635fb3678d8b8154e37d8c9cdf182fe84e60') --POLY YieldSourcePrizePool
        GROUP BY 1
        ORDER BY 1
    ),
    POLY_combo1 as (
        SELECT time_series.days_ts as days_ts, COALESCE(POLY_deposits.deposit, 0) as deposit, COALESCE(POLY_withdrawals.withdrawal, 0) as withdrawal
        FROM time_series
        left JOIN POLY_deposits ON POLY_deposits.day = time_series.days_ts
        left JOIN POLY_withdrawals ON POLY_withdrawals.day = time_series.days_ts
    ),
    POLY_combo2 as (
    SELECT days_ts, SUM((deposit) - (withdrawal)) OVER (ORDER BY POLY_combo1.days_ts) as sum1
    FROM POLY_combo1
    )

SELECT ETH.days_ts, ETH.sum1 AS ETH_balance, OP.sum1 AS OP_balance, AVAX.sum1 AS AVAX_balance , POLY.sum1 AS POLY_balance
FROM ETH_combo2 ETH
LEFT OUTER JOIN OP_combo2 OP ON ETH.days_ts = OP.days_ts
LEFT OUTER JOIN AVAX_combo2 AVAX ON ETH.days_ts = AVAX.days_ts
LEFT OUTER JOIN POLY_combo2 POLY ON ETH.days_ts = POLY.days_ts
WHERE ETH.days_TS IS NOT NULL
ORDER BY 1
)