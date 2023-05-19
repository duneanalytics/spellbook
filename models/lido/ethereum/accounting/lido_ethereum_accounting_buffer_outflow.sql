{{ config(
        alias ='buffer_outflow',
        partition_by = ['period'],
        materialized = 'table',
        file_format = 'delta',
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                "project",
                                "lido",
                                \'["gregshestakovlido", "ppclunghe", "xadcv"]\') }}'
        )
}}
--https://dune.com/queries/2488552
--ref{{'lido_ethereum_accounting_buffer_outflow'}}

SELECT evt_block_time as period, amountOfETHLocked as amount, LOWER('0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2') AS token,  evt_tx_hash
FROM {{source('lido_ethereum','WithdrawalQueueERC721_evt_WithdrawalsFinalized')}}
