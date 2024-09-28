{{ config(
    
    alias = 'app_dao_addresses',
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['created_block_time', 'dao_wallet_address', 'blockchain', 'dao', 'dao_creator_tool', 'block_month']
    )
}}

{% set project_start_date = '2023-10-29' %}

SELECT 
    'arbitrum' as blockchain,
    'aragon' as dao_creator_tool,
    bytearray_ltrim(topic1) as dao,
    bytearray_ltrim(topic1) as dao_wallet_address,
    block_time as created_block_time, 
    CAST(date_trunc('day', block_time) as DATE) as created_date, 
    CAST(date_trunc('month', block_time) as DATE) as block_month, 
    'aragon_app' as product 
FROM 
{{ source('arbitrum', 'logs') }}
{% if not is_incremental() %}
WHERE block_time >= DATE '{{project_start_date}}'
{% endif %}
{% if is_incremental() %}
WHERE block_time >= date_trunc('day', now() - interval '7' day)
{% endif %}
AND topic0 =  0xbc0b11fe649bb4d67c7fb40936163e5423f45c3ae83fbd8f8f8c75e1a3fa97af
