{{ config(
        schema = 'nft_ethereum'
        , alias = 'wallet_metrics_reservoir_floors'
        , materialized = 'table'
        , file_format = 'delta'
        , tags = ['static']
        , post_hook = '{{ hide_spells() }}'
        )
}}

-- One-time snapshot of candidate floor-ask events from the DEPRECATED reservoir community dataset
-- (reservoir.collection_floor_ask_events, frozen 2025-05-28, no updates in >1 year).
-- nft_ethereum_wallet_metrics used this as floor-price source #1 and re-scanned ~61.6 GB (91.1M rows)
-- on every run to extract the ~11.4K rows still valid in the future. We materialize just that candidate
-- slice once; the consumer re-applies valid_until_dt > current_date (+ row_number/avg) so output is
-- unchanged over time, including the handful of rows whose validity window later closes.
-- Tagged static: builds only on deploy, never on the regular schedule. If reservoir is revived,
-- trigger a redeploy of this model to refresh the snapshot.
select
    contract
  , price_decimal
  , created_at
  , valid_until_dt
  , valid_until
from {{ source('reservoir', 'collection_floor_ask_events') }}
where valid_until_dt > current_date
  and valid_until < 100000000000 -- overflow protection
