{{ config(
    schema = 'aragon_zksync',
    alias = 'app_dao_addresses',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['created_block_time', 'dao_wallet_address', 'blockchain', 'dao', 'dao_creator_tool', 'block_month'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.created_block_time')]
    )
}}


SELECT 
    'zksync' as blockchain, 
    'aragon' as dao_creator_tool, 
    dao, 
    dao as dao_wallet_address, 
    evt_block_time as created_block_time, 
    CAST(date_trunc('day', evt_block_time) as DATE) as created_date, 
    CAST(date_trunc('month', evt_block_time) as DATE) as block_month, 
    'aragon_app' as product 
FROM 
{{ source('aragon_app_zksync', 'DAOFactory_evt_DAORegistered') }}
{% if is_incremental() %}
WHERE {{incremental_predicate('evt_block_time')}}
{% endif %}