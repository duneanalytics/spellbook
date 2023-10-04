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
        version,
        positions_contract
    FROM 
    {{ ref('tigris_arbitrum_events_add_margin') }}

    UNION ALL

    SELECT 
        evt_block_time,
        position_id,
        margin,
        project_contract_address,
        version,
        positions_contract
    FROM 
    {{ ref('tigris_arbitrum_events_modify_margin') }}

    UNION ALL

    SELECT 
        evt_block_time,
        position_id,
        margin,
        project_contract_address,
        version,
        positions_contract
    FROM 
    {{ ref('tigris_arbitrum_events_open_position') }}
    WHERE open_type = 'open_position'

    UNION ALL

    SELECT 
        evt_block_time,
        position_id,
        new_margin as margin,
        project_contract_address,
        version,
        positions_contract
    FROM 
    {{ ref('tigris_arbitrum_positions_close') }}

    UNION ALL 

    SELECT 
        evt_block_time,
        position_id,
        margin,
        project_contract_address,
        version,
        positions_contract
    FROM 
    {{ ref('tigris_arbitrum_events_limit_order') }}
)

SELECT
    *
FROM 
margin
