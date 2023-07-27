WITH 

open_position_events as (
    SELECT 
        'open_position' as evt_type, 
        evt_tx_hash,
        _id as position_id, 
        evt_block_time,
        CONCAT(CAST(id as VARCHAR), 'arbitrum', 'v1.2') as position_data 
    FROM 
    {{ source('tigristrade_arbitrum', 'TradingV2_evt_PositionOpened') }} 

    UNION ALL 

    SELECT 
        'open_position' as evt_type, 
        evt_tx_hash,
        _id as position_id, 
        evt_block_time,
        CONCAT(CAST(id as VARCHAR), 'arbitrum', 'v1.3') as position_data 
    FROM 
    {{ source('tigristrade_arbitrum', 'TradingV3_evt_PositionOpened') }} 

    UNION ALL 

    SELECT 
        'open_position' as evt_type, 
        evt_tx_hash,
        _id as position_id, 
        evt_block_time,
        CONCAT(CAST(id as VARCHAR), 'arbitrum', 'v1.4') as position_data 
    FROM 
    {{ source('tigristrade_arbitrum', 'TradingV4_evt_PositionOpened') }} 

    UNION ALL 

    SELECT 
        'open_position' as evt_type, 
        evt_tx_hash,
        _id as position_id, 
        evt_block_time,
        CONCAT(CAST(id as VARCHAR), 'arbitrum', 'v1.5') as position_data 
    FROM 
    {{ source('tigristrade_arbitrum', 'TradingV5_evt_PositionOpened') }} 

    UNION ALL 

    SELECT 
        'open_position' as evt_type, 
        evt_tx_hash,
        id as position_id, 
        evt_block_time,
        CONCAT(CAST(id as VARCHAR), 'arbitrum', 'v2.1') as position_data 
    FROM 
    {{ source('tigristrade_v2_arbitrum', 'Trading_evt_PositionOpened') }}

    UNION ALL 

    SELECT 
        'open_position' as evt_type, 
        evt_tx_hash,
        id as position_id, 
        evt_block_time,
        CONCAT(CAST(id as VARCHAR), 'arbitrum', 'v2.2') as position_data 
    FROM 
    {{ source('tigristrade_v2_arbitrum', 'TradingV2_evt_PositionOpened') }}

    UNION ALL 

    SELECT 
        'open_position' as evt_type, 
        evt_tx_hash,
        id as position_id, 
        evt_block_time,
        CONCAT(CAST(id as VARCHAR), 'arbitrum', 'v2.3') as position_data 
    FROM 
    {{ source('tigristrade_v2_arbitrum', 'TradingV3_evt_PositionOpened') }}

    UNION ALL 

    SELECT 
        'open_position' as evt_type, 
        0xe739ea2b7e6174812d1c451704bf231d1612486d46358b412b4dabec531c9d8e as evt_tx_hash, 
        CAST(NULL as UINT256) AS position_id,
        DATE ('2023-07-27') as evt_block_time,
        CONCAT('test', 'arbitrum', 'v2.3') as position_data 
),

all_events as (
    SELECT * FROM open_position_events
)

SELECT 
    ae.*,
    ta.trade_type
FROM 
all_events
LEFT JOIN 
ref('tigris_arbitrum_trades') ta
    ON ae.evt_tx_hash = ta.evt_tx_hash
WHERE ta.trade_type IS NULL 