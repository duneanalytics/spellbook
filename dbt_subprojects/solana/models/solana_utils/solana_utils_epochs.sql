 {{
  config(
        schema = 'solana_utils',
        alias = 'epochs',
        materialized='table',
        file_format = 'delta',
        unique_key = ['block_slot'],
        post_hook='{{ expose_spells(\'["solana"]\',
                                    "sector",
                                    "solana_utils",
                                    \'["ilemi"]\') }}')
}}

with 
    base as (
        SELECT
            time as block_time
            , slot as block_slot
            , floor(cast(slot as double) / 432000) as epoch --round down to get current epoch slot
            , row_number() over (partition by floor(cast(slot as double) / 432000)
                order by slot asc) as first_block_epoch
            , row_number() over (partition by floor(cast(slot as double) / 432000)
                order by slot desc) as last_block_epoch
            , slot % 432000 as epoch_progress --blocks into epoch. might not always start at 0 because of skipped block slots. remember "height" shows actual non-skipped blocks but epoch follows total blocks.
        FROM {{ source('solana','blocks') }}
    )
    
SELECT 
    b.block_time
    , b.block_slot
    , b.epoch
    , case when b.first_block_epoch = 1 then true else false end as first_block_epoch
    , b.epoch_progress
    , start.block_slot as epoch_start_slot
    , next.block_slot as epoch_next_start_slot
FROM base b 
LEFT JOIN base start ON start.epoch = b.epoch AND start.first_block_epoch = 1
LEFT JOIN base next ON next.epoch = b.epoch + 1 AND next.first_block_epoch = 1
order by block_time desc  
