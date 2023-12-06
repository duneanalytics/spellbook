{% set blockchain = 'bitcoin' %}

{{ config(
        
        schema = 'inscription_' + blockchain,
        alias = 'all',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['blockchain','tx_hash']
)
}}

SELECT 'bitcoin' AS blockchain
, block_time
, block_height AS block_number
, id AS tx_hash
, index AS tx_index
, from_hex(NULL) AS tx_from
, from_hex(NULL) AS tx_to
, REGEXP_EXTRACT(REGEXP_EXTRACT(from_utf8(hex), 'ord.*?\{".*?"\} ?h[A!]'), '\{".*?"\}') AS full_inscription
FROM {{source('bitcoin', 'transactions')}}
WHERE REGEXP_EXTRACT(REGEXP_EXTRACT(from_utf8(hex), 'ord.*?\{".*?"\} ?h[A!]'), '\{".*?"\}') IS NOT NULL
AND block_height >= 767429
{% if is_incremental() %}
AND {{ incremental_predicate('block_time') }}
{% endif %}