{{ config(
        alias ='deposits',
        partition_by = ['period'],
        materialized = 'table',
        file_format = 'delta',
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                "project",
                                "lido_accounting",
                                \'["pipistrella", "adcv", "zergil1397", "lido"]\') }}'
        )
}}
--https://dune.com/queries/2011901
--ref{{'lido_accounting_ethereum_deposits'}}

	SELECT 
        block_time AS period,
        amount_staked*POWER(10,18) AS amount_staked,
        LOWER('0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2') AS token, --ETH
        tx_hash
    FROM {{ ref('staking_ethereum_deposits') }} 
    WHERE depositor_entity = 'Lido'