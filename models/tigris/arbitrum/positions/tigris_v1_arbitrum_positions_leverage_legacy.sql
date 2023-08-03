{{ config(
	tags=['legacy'],
	
    schema = 'tigris_v1_arbitrum',
    alias = alias('positions_leverage', legacy_model=True)
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
    {{ ref('tigris_arbitrum_events_open_position_legacy') }}
    WHERE protocol_version = '1'

    UNION ALL

    SELECT 
        evt_block_time,
        position_id,
        leverage,
        project_contract_address
    FROM 
    {{ ref('tigris_arbitrum_events_modify_margin_legacy') }}
     WHERE protocol_version = '1'

    UNION ALL 

    SELECT 
        evt_block_time,
        position_id,
        leverage,
        project_contract_address
    FROM 
    {{ ref('tigris_arbitrum_events_limit_order_legacy') }}
     WHERE protocol_version = '1'

)

SELECT * FROM leverage 
;