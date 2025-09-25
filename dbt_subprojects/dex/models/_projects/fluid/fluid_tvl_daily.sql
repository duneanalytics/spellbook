{{ config(
        schema = 'fluid',
        alias = 'tvl_daily',
        post_hook='{{ expose_spells(blockchains = \'["base","ethereum","polygon","arbitrum"]\',
                                      spell_type = "project", 
                                      spell_name = "fluid", 
                                      contributors = \'["Henrystats","dknugo"]\') }}'
        )
}}

    select 
        block_month
        , block_date
        , blockchain
        , project
        , version
        , dex
        , token0 
        , token1 
        , token0_symbol 
        , token1_symbol 
        , token0_balance_raw 
        , token1_balance_raw 
        , token0_balance
        , token1_balance
        , token0_balance_usd
        , token1_balance_usd
    from 
    {{ ref('fluid_base_tvl_daily') }}
    -- we need a couple of columns from the final incremental table to be able to refresh the table incrementally
    -- these columns can be confusing if displayed on dune 
    -- hence why we build a seperate table thay isn't materialized 
    -- this table selects just the relevant columns 
