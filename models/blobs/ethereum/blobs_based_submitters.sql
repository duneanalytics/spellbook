{{ config(
    schema = 'blobs',
    alias = 'based_submitters',
    materialized = 'table',
    file_format = 'delta'
)}}

SELECT evt_tx_hash AS tx_hash, evt_block_number AS block_number, 'Taiko' AS entity FROM {{ source('taikoxyz_ethereum', 'TaikoL1_evt_BlockProposed')}}