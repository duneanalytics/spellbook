{{ config(
        schema = 'rebase_ethereum',
        alias ='steth_events',post_hook='{{ expose_spells(blockchains = \'["ethereum"]\',
                                    spell_type = "sector",
                                    spell_name = "rebase",
                                    contributors = \'["hildobby"]\') }}'
)
}}

SELECT 'ethereum' AS blockchain
, 0xae7ab96520de3a18e5e111b5eaab095312d7fe84 AS token_address
, 'stETH' AS token_symbol
, evt_block_time AS block_time
, evt_block_number AS block_number
, 1 + (postTotalPooledEther - preTotalPooledEther) / (CAST(preTotalPooledEther AS DOUBLE)) * 0.9 AS rebase_rate
, evt_tx_hash AS tx_hash
, evt_index
FROM {{source('lido_ethereum','LegacyOracle_evt_PostTotalShares')}}
WHERE evt_block_time <= date('2023-05-16')

UNION ALL

SELECT 'ethereum' AS blockchain
, 0xae7ab96520de3a18e5e111b5eaab095312d7fe84 AS token_address
, 'stETH' AS token_symbol
, evt_block_time AS block_time
, evt_block_number AS block_number
, 1 + (((postTotalEther * 1e27) / CAST(postTotalShares AS DOUBLE))
    - ((preTotalEther * 1e27) / CAST(preTotalShares AS DOUBLE))
    ) / ((preTotalEther * 1e27) / CAST(preTotalShares AS DOUBLE))
    AS rebase_rate
, evt_tx_hash AS tx_hash
, evt_index
FROM {{source('lido_ethereum','steth_evt_TokenRebased')}}