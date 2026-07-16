 {{
  config(
        schema = 'solana_utils',
        alias = 'epochs',
        materialized='incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        incremental_predicates = ['DBT_INTERNAL_DEST.block_time >= now() - interval \'10\' day'],
        unique_key = ['block_slot']
        , post_hook='{{ hide_spells() }}'
)
}}

-- Solana epoch = 432000 slots (~2 days). The staking epoch models document a
-- 57-hour p99 and ~60-hour maximum epoch, so the shared 10-day lookback safely
-- contains three complete epochs. Re-emitting them hydrates the in-progress
-- epoch's `epoch_next_start_slot` once epoch+1 begins and recovers missed runs.

with
    base_raw as (
        SELECT
            time as block_time
            , slot as block_slot
            , floor(cast(slot as double) / 432000) as epoch
            , slot % 432000 as epoch_progress --blocks into epoch. might not always start at 0 because of skipped block slots. remember "height" shows actual non-skipped blocks but epoch follows total blocks.
        FROM {{ source('solana','blocks') }}
        {% if is_incremental() -%}
        WHERE time >= now() - interval '10' day
        {%- endif %}
    )
    {%- if is_incremental() %}
    -- Keep only the last 3 epochs fully. Drops any partially-observed older
    -- epoch caught by the 10-day tail so `first_block_epoch` and the `start`
    -- self-join stay correct within the window.
    , base_filtered as (
        SELECT * FROM base_raw
        WHERE epoch >= (SELECT max(epoch) - 2 FROM base_raw)
    )
    {%- else %}
    , base_filtered as (
        SELECT * FROM base_raw
    )
    {%- endif %}
    , base as (
        SELECT
            block_time
            , block_slot
            , epoch
            , row_number() over (partition by epoch order by block_slot asc) as first_block_epoch
            , row_number() over (partition by epoch order by block_slot desc) as last_block_epoch
            , epoch_progress
        FROM base_filtered
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
