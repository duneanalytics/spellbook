{{ config(
        
        schema='dex',
        alias = 'sandwiches_whitelist',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['blockchain', 'block_number', 'tx_hash'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
        post_hook='{{ expose_spells(\'["ethereum", "bnb", "base"]\',
                                "sector",
                                "dex",
                                \'["hildobby"]\') }}'
        )
}}

SELECT DISTINCT 'bnb' AS blockchain
, t.entity
, tr.block_time
, tr.block_number
, tr.tx_hash
FROM {{ source('bnb', 'traces') }} tr
INNER JOIN {{ ref('dex_sandwiches_whitelist_tags') }} t ON t.blockchain = 'bnb'
    AND tr.to = t.address
WHERE tr.block_number >= 47113816
{% if is_incremental() %}
AND {{ incremental_predicate('tr.block_time') }}
{% endif %}

UNION ALL

SELECT DISTINCT 'base' AS blockchain
, t.entity
, tr.block_time
, tr.block_number
, tr.tx_hash
FROM {{ source('base', 'traces') }} tr
INNER JOIN {{ ref('dex_sandwiches_whitelist_tags') }} t ON t.blockchain = 'base'
    AND tr.to = t.address
WHERE tr.block_number >= 27066722
{% if is_incremental() %}
AND {{ incremental_predicate('tr.block_time') }}
{% endif %}

UNION ALL

SELECT DISTINCT 'ethereum' AS blockchain
, t.entity
, tr.block_time
, tr.block_number
, tr.tx_hash
FROM {{ source('ethereum', 'traces') }} tr
INNER JOIN {{ ref('dex_sandwiches_whitelist_tags') }} t ON t.blockchain = 'ethereum'
    AND tr.to = t.address
WHERE tr.block_number >= 21959593
{% if is_incremental() %}
AND {{ incremental_predicate('tr.block_time') }}
{% endif %}