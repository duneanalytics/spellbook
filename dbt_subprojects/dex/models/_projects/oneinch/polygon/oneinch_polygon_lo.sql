{% set blockchain = 'polygon' %}

{{
    config(
        schema = 'oneinch_' + blockchain,
        alias = 'lo',
        materialized = 'view',
    )
}}

-- TEMP VIEW. WILL BE DELETED IN THE NEXT PR --

select * from {{ source('oneinch_' + blockchain, 'lop') }}