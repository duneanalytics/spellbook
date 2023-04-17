{{ config(
        alias ='deposits',
        partition_by = ['period'],
        materialized = 'table',
        file_format = 'delta',
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                "project",
                                "lido",
                                \'["pipistrella", "adcv", "zergil1397", "lido"]\') }}'
        )
}}
--https://dune.com/queries/2011901
--ref{{'lido_ethereum_accounting_deposits'}} 

 
  select    block_time AS period,
            cast(value as double) as amount_staked,
            LOWER('0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2') AS token, --ETH
            tx_hash
  from {{source('ethereum','traces')}}
  where `to` = lower('0x00000000219ab540356cBB839Cbe05303d7705Fa')
    and from = lower('0xae7ab96520de3a18e5e111b5eaab095312d7fe84')
    AND (LOWER(call_type) NOT IN ('delegatecall', 'callcode', 'staticcall') or call_type is null)
    AND success
  