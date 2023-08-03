{{ config(
    tags=['dunesql'],
    schema = 'tigris_v1_arbitrum',
    alias = alias('positions_leverage')
    )
 }}

WITH 

leverage as (
    SELECT 
        evt_block_time,
        position_id,
        leverage,
        project_contract_address
    FROM 
    {{ ref('tigris_arbitrum_events_open_position') }}
     WHERE protocol_version = '1'

    UNION ALL

    SELECT 
        evt_block_time,
        position_id,
        leverage,
        project_contract_address
    FROM 
    {{ ref('tigris_arbitrum_events_modify_margin') }}
     WHERE protocol_version = '1'

    UNION ALL 

    SELECT 
        evt_block_time,
        position_id,
        leverage,
        project_contract_address
    FROM 
    {{ ref('tigris_arbitrum_events_limit_order') }}
     WHERE protocol_version = '1'

)

SELECT * FROM leverage 
