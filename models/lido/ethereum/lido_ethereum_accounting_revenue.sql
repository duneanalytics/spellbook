{{ config(
        alias ='revenue',
        partition_by = ['period'],
        materialized = 'table',
        file_format = 'delta',
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                "project",
                                "lido",
                                \'["pipistrella", "adcv", "zergil1397", "lido"]\') }}'
        )
}}
--https://dune.com/queries/2011922
--ref{{'lido_accounting_revenue'}}

with oracle_txns AS ( 
    SELECT
        evt_block_time AS period,
        (CAST(postTotalPooledEther AS DOUBLE)-CAST(preTotalPooledEther AS DOUBLE)) lido_rewards,
        evt_tx_hash
    FROM {{source('lido_ethereum','LegacyOracle_evt_PostTotalShares')}}
    ORDER BY 1 DESC
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
        LOWER('0xae7ab96520DE3A18E5e111B5EaAb095312D7fE84') AS token,
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
    ORDER BY 1,2 
