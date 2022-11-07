{{ config(
        alias ='transfers',
        partition_by='block_date',
        materialized='incremental',
        file_format = 'delta',
        post_hook='{{ expose_spells(\'["ethereum", "bnb", "avalanche_c", "gnosis", "optimism", "arbitrum", "polygon"]\',
                                    "sector",
                                    "nft",
                                    \'["hildobby"]\') }}',
        unique_key = ['unique_transfer_id']
)
}}

SELECT 'ethereum' AS blockchain
, block_time
, block_date
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
UNION ALL
SELECT 'bnb' AS blockchain
, block_time
, block_date
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
UNION ALL
SELECT 'avalanche_c' AS blockchain
, block_time
, block_date
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
UNION ALL
SELECT 'gnosis' AS blockchain
, block_time
, block_date
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
UNION ALL
SELECT 'optimism' AS blockchain
, block_time
, block_date
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
UNION ALL
SELECT 'arbitrum' AS blockchain
, block_time
, block_date
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
UNION ALL
SELECT 'polygon' AS blockchain
, block_time
, block_date
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