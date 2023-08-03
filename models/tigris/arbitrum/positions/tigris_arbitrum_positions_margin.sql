{{ config(
    tags=['dunesql'],
    schema = 'tigris_arbitrum',
    alias = alias('positions_margin')
    )
 }}

WITH 

margin as (
    SELECT 
        evt_block_time,
        position_id,
        margin,
        project_contract_address,
        version
    FROM 
    {{ ref('tigris_arbitrum_events_add_margin') }}

    UNION ALL

    SELECT 
        evt_block_time,
        position_id,
        margin,
        project_contract_address,
        version
    FROM 
    {{ ref('tigris_arbitrum_events_modify_margin') }}

    UNION ALL

    SELECT 
        evt_block_time,
        position_id,
        margin,
        project_contract_address,
        version
    FROM 
    {{ ref('tigris_arbitrum_events_open_position') }}
    WHERE open_type = 'open_position'

    UNION ALL

    SELECT 
        evt_block_time,
        position_id,
        new_margin as margin,
        project_contract_address,
        version
    FROM 
    {{ ref('tigris_arbitrum_positions_close') }}

    UNION ALL 

    SELECT 
        evt_block_time,
        position_id,
        margin,
        project_contract_address,
        version
    FROM 
    {{ ref('tigris_arbitrum_events_limit_order') }}
)

SELECT
    m.*, 
    c.positions_contract
FROM 
margin m
INNER JOIN 
{{ ref('tigris_arbitrum_events_contracts_positions') }} c 
    ON m.project_contract_address = c.trading_contract
    AND m.version = c.trading_contract_version
