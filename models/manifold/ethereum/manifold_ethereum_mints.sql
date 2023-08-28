{{ config(tags=['dunesql'],
    schema = 'manifold_ethereum',
    alias = alias('mints'),
    partition_by = ['block_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['tx_hash', 'evt_index']
    post_hook='{{ expose_spells(\'["ethereum"]\',
                                "project",
                                "manifold",
                                \'["hildobby"]\') }}'
    )
}}

{% set manifold_start_date = '2023-01-01' %}

SELECT 'claim' AS mint_type
, 'erc721' AS nft_type
, evt_block_time AS block_time
, date_trunc('day', evt_block_time) AS block_date
, evt_block_number AS block_number
, 1 AS amount
, 0 AS price
, CAST(NULL AS varbinary) AS currency_address
, NULL AS currency_symbol
, creatorContract AS nft_contract_address
, evt_tx_hash AS tx_hash
, evt_index
, contract_address
FROM {{ source('manifold_ethereum','ERC721LazyClaim_evt_ClaimMint') }}
{% if not is_incremental() %}
WHERE evt_block_time >= TIMESTAMP '{{manifold_start_date}}'
{% endif %}
{% if is_incremental() %}
WHERE evt_block_time >= date_trunc('day', now() - interval '7' day)
{% endif %}

UNION ALL

SELECT 'claim' AS mint_type
, 'erc721' AS nft_type
, evt_block_time AS block_time
, date_trunc('day', evt_block_time) AS block_date
, evt_block_number AS block_number
, mintCount AS amount
, 0 AS price
, CAST(NULL AS varbinary) AS currency_address
, NULL AS currency_symbol
, creatorContract AS nft_contract_address
, evt_tx_hash AS tx_hash
, evt_index
, contract_address
FROM {{ source('manifold_ethereum','ERC721LazyClaim_evt_ClaimMintBatch') }}
{% if not is_incremental() %}
WHERE evt_block_time >= TIMESTAMP '{{manifold_start_date}}'
{% endif %}
{% if is_incremental() %}
WHERE evt_block_time >= date_trunc('day', now() - interval '7' day)
{% endif %}

UNION ALL

SELECT 'claim' AS mint_type
, 'erc1155' AS nft_type
, evt_block_time AS block_time
, date_trunc('day', evt_block_time) AS block_date
, evt_block_number AS block_number
, 1 AS amount
, 0 AS price
, CAST(NULL AS varbinary) AS currency_address
, NULL AS currency_symbol
, creatorContract AS nft_contract_address
, evt_tx_hash AS tx_hash
, evt_index
, contract_address
FROM  {{ source('manifold_ethereum','ERC1155LazyClaim_evt_ClaimMint') }}
{% if not is_incremental() %}
WHERE evt_block_time >= TIMESTAMP '{{manifold_start_date}}'
{% endif %}
{% if is_incremental() %}
WHERE evt_block_time >= date_trunc('day', now() - interval '7' day)
{% endif %}

UNION ALL

SELECT 'claim' AS mint_type
, 'erc1155' AS nft_type
, evt_block_time AS block_time
, date_trunc('day', evt_block_time) AS block_date
, evt_block_number AS block_number
, mintCount AS amount
, 0 AS price
, CAST(NULL AS varbinary) AS currency_address
, NULL AS currency_symbol
, creatorContract AS nft_contract_address
, evt_tx_hash AS tx_hash
, evt_index
, contract_address
FROM {{ source('manifold_ethereum','ERC1155LazyClaim_evt_ClaimMintBatch') }}
{% if not is_incremental() %}
WHERE evt_block_time >= TIMESTAMP '{{manifold_start_date}}'
{% endif %}
{% if is_incremental() %}
WHERE evt_block_time >= date_trunc('day', now() - interval '7' day)
{% endif %}