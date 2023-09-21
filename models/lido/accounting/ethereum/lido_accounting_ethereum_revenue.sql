{{ config(
        alias = alias('revenue'),
        tags = ['dunesql'], 
        partition_by = ['period'],
        materialized = 'table',
        file_format = 'delta',
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                "project",
                                "lido_accounting",
                                \'["pipistrella", "adcv", "zergil1397", "lido"]\') }}'
        )
}}

--https://dune.com/queries/2011922
--ref{{'lido_accounting_revenue'}}

with 
addresses AS (
select * from (values
(0x3e40d73eb977dc6a537af587d48316fee66e9c8c, 'Aragon'),
(0x55032650b14df07b85bF18A3a3eC8E0Af2e028d5,  'NO'),
(0x8B3f33234ABD88493c0Cd28De33D583B70beDe35,  'InsuranceFund')
) as list(address, name)
 ),

oracle_txns AS ( 
    SELECT
        evt_block_time AS period,
        (CAST(postTotalPooledEther AS DOUBLE)-CAST(preTotalPooledEther AS DOUBLE)) lido_rewards,
        evt_tx_hash
    FROM {{source('lido_ethereum','LegacyOracle_evt_PostTotalShares')}}
    WHERE evt_block_time <= timestamp '2023-05-16 00:00' 
    ORDER BY 1 DESC
),


oraclev2_txns as (  
    SELECT period, sum(treasury_revenue) as treasury_revenue, sum(operators_revenue) as operators_revenue, sum(insurance_revenue) as insurance_revenue, evt_tx_hash 
    FROM (
    SELECT 
        o.evt_block_time as period,
        case when t.to in  (select address from addresses where name = 'Aragon') then cast(t.value as double) else 0 end AS treasury_revenue,
        case when t.to in  (select address from addresses where name = 'NO') then cast(t.value as double) else 0 end AS operators_revenue,
        case when t.to in  (select address from addresses where name = 'InsuranceFund') then cast(t.value as double) else 0 end as insurance_revenue,
        o.evt_tx_hash
    FROM {{source('lido_ethereum','AccountingOracle_evt_ProcessingStarted')}} o
    left join {{source('lido_ethereum','steth_evt_Transfer')}} t on o.evt_tx_hash = t.evt_tx_hash 
            and t."from" = 0x0000000000000000000000000000000000000000
            and to in (select address from addresses)
    ) group by 1,5
),


protocol_fee AS (
    SELECT 
        DATE_TRUNC('day', evt_block_time) AS period, 
        LEAD(DATE_TRUNC('day', evt_block_time), 1, NOW()) OVER (ORDER BY DATE_TRUNC('day', evt_block_time)) AS next_period,
        CAST(feeBasisPoints AS DOUBLE)/10000 AS points
    FROM {{source('lido_ethereum','steth_evt_FeeSet')}}
),

protocol_fee_distribution AS (
    SELECT 
        DATE_TRUNC('day', evt_block_time) AS period, 
        LEAD(DATE_TRUNC('day', evt_block_time), 1, NOW()) OVER (ORDER BY DATE_TRUNC('day', evt_block_time)) AS next_period,
        CAST(insuranceFeeBasisPoints AS DOUBLE)/10000 AS insurance_points,
        CAST(operatorsFeeBasisPoints AS DOUBLE)/10000 AS operators_points,
        CAST(treasuryFeeBasisPoints AS DOUBLE)/10000 AS treasury_points
    FROM {{source('lido_ethereum','steth_evt_FeeDistributionSet')}}
)


    SELECT  
        oracle_txns.period AS period, 
        oracle_txns.evt_tx_hash,
        0xae7ab96520DE3A18E5e111B5EaAb095312D7fE84 AS token,
        lido_rewards AS total,
        protocol_fee.points AS protocol_fee,
        protocol_fee_distribution.insurance_points AS insurance_fee,
        protocol_fee_distribution.operators_points AS operators_fee,
        protocol_fee_distribution.treasury_points AS treasury_fee,
        (1 - protocol_fee.points)*lido_rewards AS depositors_revenue,
        protocol_fee.points*protocol_fee_distribution.treasury_points*lido_rewards AS treasury_revenue,
        protocol_fee.points*protocol_fee_distribution.insurance_points*lido_rewards AS insurance_revenue,
        protocol_fee.points*protocol_fee_distribution.operators_points*lido_rewards AS operators_revenue
    FROM oracle_txns
    LEFT JOIN protocol_fee ON DATE_TRUNC('day', oracle_txns.period) >= protocol_fee.period AND DATE_TRUNC('day', oracle_txns.period) < protocol_fee.next_period
    LEFT JOIN protocol_fee_distribution ON DATE_TRUNC('day', oracle_txns.period) >= protocol_fee_distribution.period AND DATE_TRUNC('day', oracle_txns.period) < protocol_fee_distribution.next_period

    union all

    SELECT  
        oracle_txns.period AS period, 
        oracle_txns.evt_tx_hash,
        0xae7ab96520DE3A18E5e111B5EaAb095312D7fE84 AS token,
        oracle_txns.treasury_revenue + oracle_txns.operators_revenue + 	oracle_txns.insurance_revenue AS total,
        protocol_fee.points AS protocol_fee,
        protocol_fee_distribution.insurance_points AS insurance_fee,
        protocol_fee_distribution.operators_points AS operators_fee,
        protocol_fee_distribution.treasury_points AS treasury_fee,
        10*(1 - protocol_fee.points)*(oracle_txns.treasury_revenue + oracle_txns.operators_revenue + 	oracle_txns.insurance_revenue) AS depositors_revenue,
        oracle_txns.treasury_revenue AS treasury_revenue,
        oracle_txns.insurance_revenue AS insurance_revenue,
        oracle_txns.operators_revenue AS operators_revenue
    FROM oraclev2_txns oracle_txns
    LEFT JOIN protocol_fee ON DATE_TRUNC('day', oracle_txns.period) >= protocol_fee.period AND DATE_TRUNC('day', oracle_txns.period) < protocol_fee.next_period
    LEFT JOIN protocol_fee_distribution ON DATE_TRUNC('day', oracle_txns.period) >= protocol_fee_distribution.period AND DATE_TRUNC('day', oracle_txns.period) < protocol_fee_distribution.next_period

