{{ 
  config(
        schema = 'dex_aggregator_jupiter_solana',
        alias = 'trades',
        materialized='view'
    )
}}

/*
    -- jupiter solana swaps has been a standalone table for awhile
    -- to avoid breaking schema / queries, introduce staging view prior to final dex aggregator trades table
    -- all future projects will build upstream proper columns for final schema
*/

select
    'solana' AS blockchain
    , 'jupiter' AS project
    , jup_version AS version
    , cast(jup_version as varchar) AS version_name
    , block_month
    , cast(date_trunc('day', block_time) AS DATE) as block_date
    , block_time
    , block_slot
    , null as trade_source
    , output_symbol as token_bought_symbol
    , input_symbol as token_sold_symbol
    , token_pair
    , 
from {{ ref('jupiter_solana_aggregator_swaps') }}