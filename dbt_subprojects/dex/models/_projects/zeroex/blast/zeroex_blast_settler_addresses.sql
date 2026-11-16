{{
    config(
        schema = 'zeroex_blast',
        alias = 'settler_addresses',
        materialized='table',
        file_format ='delta',
        tags=['static'],
        post_hook='{{ hide_spells() }}'
        )
}}

-- Use the zeroex_settler_addresses macro to generate the settler addresses for blast
{{ zeroex_settler_addresses('blast') }} 