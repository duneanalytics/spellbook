{{ config(
	tags=['legacy'],
	
        alias = alias('buffer_outflow', legacy_model=True),
        partition_by = ['day'],
        materialized = 'table',
        file_format = 'delta',
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                "project",
                                "lido_accounting",
                                \'["gregshestakovlido", "ppclunghe", "xadcv"]\') }}'
        )
}}
--https://dune.com/queries/2488552
--ref{{'lido_accounting_ethereum_buffer_outflow'}}

SELECT evt_block_time as period, amountOfETHLocked as amount, LOWER('0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2') AS token,  evt_tx_hash, date_trunc('day', evt_block_time) as day
FROM {{source('lido_ethereum','WithdrawalQueueERC721_evt_WithdrawalsFinalized')}}