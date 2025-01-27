{{ config(
        schema='lido_accounting_ethereum',
        alias = 'buffer_outflow',

        materialized = 'table',
        file_format = 'delta',
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                "project",
                                "lido_accounting",
                                \'["pipistrella", "adcv", "zergil1397"]\') }}'
        )
}}

SELECT evt_block_time as period, amountOfETHLocked as amount, 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2 AS token,  evt_tx_hash, date_trunc('day', evt_block_time) as day
FROM {{source('lido_ethereum','WithdrawalQueueERC721_evt_WithdrawalsFinalized')}}
