{{ config(
	tags=['legacy'],
    alias = alias('app_dao_addresses', legacy_model=True),
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['created_block_time', 'dao_wallet_address', 'blockchain', 'dao', 'dao_creator_tool', 'block_month']
    )
}}

{% set project_start_date = '2023-02-27' %}

-- dune query here  https://dune.com/queries/2100647/3457591

SELECT 
    'ethereum' as blockchain, 
    'aragon' as dao_creator_tool, 
    CONCAT('0x', SUBSTRING(topic2, 27, 40)) as dao, 
    CONCAT('0x', SUBSTRING(topic2, 27, 40)) as dao_wallet_address, 
    block_time as created_block_time, 
    CAST(date_trunc('day', block_time) as DATE) as created_date, 
    CAST(date_trunc('month', block_time) as DATE) as block_month, 
    'aragon_app' as product 
FROM 
{{ source('ethereum', 'logs') }}
{% if not is_incremental() %}
WHERE block_time >= '{{project_start_date}}'
{% endif %}
{% if is_incremental() %}
WHERE block_time >= date_trunc("day", now() - interval '1 week')
{% endif %}
AND topic1 = LOWER('0xbc0b11fe649bb4d67c7fb40936163e5423f45c3ae83fbd8f8f8c75e1a3fa97af')