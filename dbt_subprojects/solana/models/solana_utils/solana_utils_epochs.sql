 {{
  config(
        schema = 'solana_utils',
        alias = 'epochs',
        materialized='incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
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
            , slot % 432000 as epoch_progress --blocks into epoch. might not always start at 0 because of skipped block slots. remember "height" shows actual non-skipped blocks but epoch follows total blocks.
            , floor(cast(slot as double) / 432000) * 432000 as epoch_start_slot
            , floor(cast(slot as double) / 432000) * 432000 + 431999 as epoch_end_slot
        FROM {{ source('solana','blocks') }}
        {% if is_incremental() %}
        WHERE {{incremental_predicate('time')}}
        {% endif %}
    )
    
SELECT 
    block_time
    , block_slot
    , epoch
    , case when first_block_epoch = 1 then true else false end as first_block_epoch
    , epoch_progress
    , epoch_start_slot
    , epoch_end_slot
FROM base
order by time desc