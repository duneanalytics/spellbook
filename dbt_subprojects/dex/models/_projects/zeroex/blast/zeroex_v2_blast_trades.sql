{{  config(
    schema = 'zeroex_v2_blast',
    alias = 'trades',
    materialized='table',
    partition_by = ['block_month'],
    file_format ='delta',
    tags=['static'],
    post_hook='{{ hide_spells() }}'
)}}

{{
    zeroex_settler_agg(
        blockchain = 'blast'
    )
}}
