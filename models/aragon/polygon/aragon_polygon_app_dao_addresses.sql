{{ config(
    tags=['dunesql'],
    alias = alias('app_dao_addresses'),
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['created_block_time', 'dao_wallet_address', 'blockchain', 'dao', 'dao_creator_tool', 'block_month']
    )
}}

-- query on dune explorer https://dune.com/queries/2412802

{% set project_start_date = '2023-02-27' %}


SELECT 
    'polygon' as blockchain, 
    'aragon' as dao_creator_tool, 
    dao, 
    dao as dao_wallet_address, 
    evt_block_time as created_block_time, 
    CAST(date_trunc('day', evt_block_time) as DATE) as created_date, 
    CAST(date_trunc('month', evt_block_time) as DATE) as block_month, 
    'aragon_app' as product 
FROM 
{{ source('aragon_app_polygon', 'DAORegistry_evt_DAORegistered') }}
{% if not is_incremental() %}
WHERE evt_block_time >= DATE '{{project_start_date}}'
{% endif %}
{% if is_incremental() %}
WHERE evt_block_time >= date_trunc('day', now() - interval '7' day)
{% endif %}