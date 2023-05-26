{{ config(
        alias ='deposits',
        partition_by = ['day'],
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

	SELECT  block_time as period, 
        sum(cast(value as DOUBLE)) as amount_staked, 
        LOWER('0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2') AS token,
        tx_hash,
        date_trunc('day', block_time) as day 
        FROM  {{source('ethereum','traces')}} 
        WHERE to = LOWER('0x00000000219ab540356cbb839cbe05303d7705fa')
        AND call_type = 'call'
        AND success = True 
        AND `from` in (LOWER('0xae7ab96520de3a18e5e111b5eaab095312d7fe84'), LOWER('0xB9D7934878B5FB9610B3fE8A5e441e8fad7E293f'), LOWER('0xFdDf38947aFB03C621C71b06C9C70bce73f12999'))
        group by 1,3,4,5
                