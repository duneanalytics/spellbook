{{ config(
        alias = alias('buffer_inflow'),
        tags = ['dunesql'], 
        materialized = 'table',
        file_format = 'delta',
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                "project",
                                "lido_accounting",
                                \'["gregshestakovlido", "ppclunghe", "xadcv"]\') }}'
        )
}}
--https://dune.com/queries/2488514
--ref{{'lido_accounting_ethereum_buffer_inflow'}}


SELECT  evt_block_time as period, amount as amount,0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2 as token, evt_tx_hash, date_trunc('day', evt_block_time) as day
FROM {{source('lido_ethereum','steth_evt_Submitted')}}

union all

SELECT evt_block_time, amount, 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2, evt_tx_hash, date_trunc('day', evt_block_time) as day
FROM {{source('lido_ethereum','steth_evt_ELRewardsReceived')}}

union all 

SELECT evt_block_time, amount , 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2, evt_tx_hash, date_trunc('day', evt_block_time) as day
FROM {{source('lido_ethereum','steth_evt_WithdrawalsReceived')}}