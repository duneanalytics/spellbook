{{ config( 
    schema = 'oneinch_fantom',
    alias = alias('calls_transfers', legacy_model=True),
    tags = ['legacy'],
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['tx_hash', 'start', '_transfer_trace_address_not_null', 'block_month']
    )
}}


-- DUMMY TABLE, WILL BE REMOVED SOON


select 
    1 as tx_hash
    , 1 as block_time
    , 1 as tx_from
    , 1 as start
    , 1 as transfer_trace_address
    , 1 as token_address
    , 1 as amount
    , 1 as transfer_from
    , 1 as transfer_to
    , 1 as caller
    , 1 as call_selector
    , 1 as call_success
    , 1 as tx_success
    , 1 as rn_ta_asc
    , 1 as rn_ta_desc
    , 1 as block_month
    , 1 as _transfer_trace_address_not_null
{% if is_incremental() %}
    where 1=1
{% else %}
    where 1=1
{% endif %}
