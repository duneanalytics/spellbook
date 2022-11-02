{{ config(
        alias ='transfers',
        partition_by='block_date',
        materialized='incremental',
        file_format = 'delta',
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                    "sector",
                                    "nft",
                                    \'["hildobby"]\') }}',
        unique_key = ['unique_trade_id']
)
}}

SELECT 'ethereum' AS blockchain
, block_time
, block_number
, token_standard
, transfer_type
, evt_index
, contract_address
, token_id
, amount
, from
, to
, tx_hash
, unique_transfer_id
FROM {{ ref('nft_ethereum_transfers') }}
UNION
SELECT 'bnb' AS blockchain
, block_time
, block_number
, token_standard
, transfer_type
, evt_index
, contract_address
, token_id
, amount
, from
, to
, tx_hash
, unique_transfer_id
FROM {{ ref('nft_bnb_transfers') }}
UNION
SELECT 'avalanche' AS blockchain
, block_time
, block_number
, token_standard
, transfer_type
, evt_index
, contract_address
, token_id
, amount
, from
, to
, tx_hash
, unique_transfer_id
FROM {{ ref('nft_avalanche_c_transfers') }}
UNION
SELECT 'gnosis' AS blockchain
, block_time
, block_number
, token_standard
, transfer_type
, evt_index
, contract_address
, token_id
, amount
, from
, to
, tx_hash
, unique_transfer_id
FROM {{ ref('nft_gnosis_transfers') }}
UNION
SELECT 'optimism' AS blockchain
, block_time
, block_number
, token_standard
, transfer_type
, evt_index
, contract_address
, token_id
, amount
, from
, to
, tx_hash
, unique_transfer_id
FROM {{ ref('nft_optimism_transfers') }}
UNION
SELECT 'arbitrum' AS blockchain
, block_time
, block_number
, token_standard
, transfer_type
, evt_index
, contract_address
, token_id
, amount
, from
, to
, tx_hash
, unique_transfer_id
FROM {{ ref('nft_arbitrum_transfers') }}
UNION
SELECT 'polygon' AS blockchain
, block_time
, block_number
, token_standard
, transfer_type
, evt_index
, contract_address
, token_id
, amount
, from
, to
, tx_hash
, unique_transfer_id
FROM {{ ref('nft_polygon_transfers') }}