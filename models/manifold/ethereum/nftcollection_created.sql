{{ config(
    alias = 'nftcollection_created',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_time', 'tx_hash', 'depositor_address'],
    post_hook='{{ expose_spells(\'["ethereum"]\',
                                "sector",
                                "manifold",
                                \'["hildobby"]\') }}')
}}

SELECT 'Manifold' AS tool
, 'Tool Assisted' AS mint_type
, 'Burn Redeem' AS nft_type
, 'erc1155' AS nft_standard
, evt_block_time AS block_time
, evt_block_number AS block_number
, creatorContract AS nft_contract_address
, NULL AS collection_size
, initializer AS creator
, contract_address AS tool_contract_address
, evt_tx_hash AS tx_hash
FROM {{ source('manifold_ethereum','ERC1155BurnRedeem_evt_BurnRedeemInitialized') }}

UNION ALL

SELECT 'Manifold' AS tool
, 'Tool Assisted' AS mint_type
, 'Pay Claim' AS nft_type
, 'erc1155' AS nft_standard
, evt_block_time AS block_time
, evt_block_number AS block_number
, creatorContract AS nft_contract_address
, NULL AS collection_size
, initializer AS creator
, contract_address AS tool_contract_address
, evt_tx_hash AS tx_hash
FROM {{ source('manifold_ethereum','ERC1155LazyPayableClaim_evt_ClaimInitialized') }}

UNION ALL

SELECT 'Manifold' AS tool
, 'Tool Assisted' AS mint_type
, 'Pay Claim v2' AS nft_type
, 'erc1155' AS nft_standard
, evt_block_time AS block_time
, evt_block_number AS block_number
, creatorContract AS nft_contract_address
, NULL AS collection_size
, initializer AS creator
, contract_address AS tool_contract_address
, evt_tx_hash AS tx_hash
FROM {{ source('manifold_ethereum','ERC1155LazyPayableClaimV2_evt_ClaimInitialized') }}

UNION ALL

SELECT 'Manifold' AS tool
, 'Tool Assisted' AS mint_type
, 'Pay Claim' AS nft_type
, 'erc721' AS nft_standard
, evt_block_time AS block_time
, evt_block_number AS block_number
, creatorContract AS nft_contract_address
, NULL AS collection_size
, initializer AS creator
, contract_address AS tool_contract_address
, evt_tx_hash AS tx_hash
FROM {{ source('manifold_ethereum','ERC721LazyPayableClaim_evt_ClaimInitialized') }}

UNION ALL

SELECT 'Manifold' AS tool
, 'Tool Assisted' AS mint_type
, 'Pay Claim v2' AS nft_type
, 'erc721' AS nft_standard
, evt_block_time AS block_time
, evt_block_number AS block_number
, creatorContract AS nft_contract_address
, NULL AS collection_size
, initializer AS creator
, contract_address AS tool_contract_address
, evt_tx_hash AS tx_hash
FROM {{ source('manifold_ethereum','ERC721LazyPayableClaimV2_evt_ClaimInitialized') }}

UNION ALL

SELECT 'Manifold' AS tool
, 'Tool Assisted' AS mint_type
, 'Edition' AS nft_type
, 'erc721' AS nft_standard
, evt_block_time AS block_time
, evt_block_number AS block_number
, creator AS nft_contract_address
, maxSupply AS collection_size
, caller AS creator
, contract_address AS tool_contract_address
, evt_tx_hash AS tx_hash
FROM {{ source('manifold_ethereum','ManifoldERC721Edition_evt_SeriesCreated') }}