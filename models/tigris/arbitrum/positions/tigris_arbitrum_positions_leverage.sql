{{ config(
    tags=['dunesql'],
    schema = 'tigris_arbitrum',
    alias = alias('positions_leverage')
    )
 }}

WITH 

leverage as (
    SELECT 
        evt_block_time,
        position_id,
        leverage,
        project_contract_address,
        version
    FROM 
    {{ ref('tigris_arbitrum_events_open_position') }}
    WHERE open_type = 'open_position'

    UNION ALL

    SELECT 
        evt_block_time,
        position_id,
        leverage,
        project_contract_address,
        version 
    FROM 
    {{ ref('tigris_arbitrum_events_modify_margin') }}

    UNION ALL 

    SELECT 
        evt_block_time,
        position_id,
        leverage,
        project_contract_address,
        version
    FROM 
    {{ ref('tigris_arbitrum_events_limit_order') }}
)

SELECT 
    l.*, 
    c.positions_contract
FROM 
leverage l 
INNER JOIN 
{{ ref('tigris_arbitrum_events_contracts_positions') }} c 
    ON l.project_contract_address = c.trading_contract
    AND l.version = c.trading_contract_version
