{{ config(
    schema = 'uniswap'
    , alias = 'trades'
    , partition_by = ['block_month', 'blockchain', 'project']
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , unique_key = ['blockchain', 'project', 'version', 'tx_hash', 'evt_index']
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    , post_hook='{{ expose_spells(blockchains = \'["ethereum", "arbitrum", "optimism", "polygon", "bnb", "base", "celo", "avalanche_c", "unichain"]\',
                                      spell_type = "project", 
                                      spell_name = "uniswap", 
                                      contributors = \'["jeff-dude", "mtitus6", "Henrystats", "chrispearcx", "wuligy", "tomfutago", "phu"]\') }}'
    )
}}

{% set v4_models = [
    ref('uniswap_v4_arbitrum_base_trades')
    , ref('uniswap_v4_avalanche_c_base_trades')
    , ref('uniswap_v4_base_base_trades')
    , ref('uniswap_v4_blast_base_trades')
    , ref('uniswap_v4_bnb_base_trades')
    , ref('uniswap_v4_ethereum_base_trades')
    , ref('uniswap_v4_ink_base_trades')
    , ref('uniswap_v4_optimism_base_trades')
    , ref('uniswap_v4_polygon_base_trades')
    , ref('uniswap_v4_unichain_base_trades')
    , ref('uniswap_v4_worldchain_base_trades')
    , ref('uniswap_v4_zora_base_trades')
] %}

{% set bunni_models = [
    {"source": source('bunni_v2_base', 'bunnihook_evt_swap'), "chain": "base"}
    , {"source": source('bunni_v2_arbitrum', 'bunnihook_evt_swap'), "chain": "arbitrum"}
    , {"source": source('bunni_v2_ethereum', 'bunnihook_evt_swap'), "chain": "ethereum"}
    , {"source": source('bunni_v2_unichain', 'bunnihook_evt_swap'), "chain": "unichain"}
    , {"source": source('bunni_v2_bnb', 'bunnihook_evt_swap'), "chain": "bnb"}
] %}

with 

v4_trades as (
    {% for model in v4_models %}
    select
        blockchain
        , tx_hash 
        , evt_index 
        , fee 
        , block_time 
        , block_date 
        , block_number 
    from
        {{ model }}
    {% if is_incremental() %}
    where
        {{ incremental_predicate('block_time') }}
    {% endif %}
    {% if not loop.last %}
    union all 
    {% endif %}
    {% endfor %}
),

bunni_fees as (
    {% for model in bunni_models %}
    select
        '{{ model.chain }}' as blockchain
        , evt_tx_hash  as tx_hash
        , fee  
        , evt_block_date as block_date
        , id 
        , evt_index + 1 as evt_index
        , 'bunni' as hooks 
    from
        {{ model.source }}
    {% if is_incremental() %}
    where {{ incremental_predicate('evt_block_time') }}
    {% endif %}
    {% if not loop.last %}
    union all 
    {% endif %}
    {% endfor %}
),

flaunch_hookswap as (
    select 
        * 
    from 
    {{ source('flaunch_base', 'positionmanager_v2_evt_hookswap') }} 
    {% if is_incremental() %}
    where {{ incremental_predicate('evt_block_time') }}
    {% endif %}
),

flaunch_hookfee as (
    select 
        * 
    from 
    {{ source('flaunch_base', 'positionmanager_v2_evt_hookfee') }} 
    {% if is_incremental() %}
    where {{ incremental_predicate('evt_block_time') }}
    {% endif %}
),

flaunch_poolswap as (
    select 
        * 
    from 
    {{ source('flaunch_base', 'positionmanager_v2_evt_poolswap') }} 
    {% if is_incremental() %}
    where {{ incremental_predicate('evt_block_time') }}
    {% endif %}
),

univ4_base_trades as (
    select 
        *
        , 'base' as blockchain 
    from 
    {{ source('uniswap_v4_base', 'PoolManager_evt_Swap') }} 
    {% if is_incremental() %}
    where {{ incremental_predicate('evt_block_time') }}
    {% endif %}
),

flaunch_prep1 as (
    select
        a.evt_block_date 
        , a.evt_tx_hash
        , a.evt_index
        , a.id
        , ABS(ROUND(
            CASE
                WHEN b.feeAmount0 IS NOT NULL and b.feeAmount1 = 0
                    THEN CAST(b.feeAmount0 AS DOUBLE) / CAST(a.amount0 AS DOUBLE)
                WHEN b.feeAmount1 IS NOT NULL and b.feeAmount0 = 0
                    THEN CAST(b.feeAmount1 AS DOUBLE) / CAST(a.amount1 AS DOUBLE)
            END, 10)) AS fee
        , 'base' as blockchain
        , b.feeAmount0
        , b.feeAmount1
    from
    flaunch_hookswap a
    left join 
    ( 
        select 
            * 
            , evt_index - 1 as evt_2 
        from 
        flaunch_hookfee
    ) b
        on a.evt_tx_hash = b.evt_tx_hash
        and a.evt_block_date = b.evt_block_date 
        and a.id = b.id
        and a.evt_index = b.evt_2
),

flaunch_fees as (
    select 
        evt_block_date 
        , evt_tx_hash 
        , id 
        , min(fee) as fee          -- we can't join on index, for cases where there are multiple combo of uniamount0, uniamount1 choose just one to avoid duplicates, this never happens, just future proofing by aggregating
        , blockchain 
        , uniAmount0 
        , uniAmount1 
        , 'flaunch' as hooks 
    from (
    select
        a.*
        , b.uniAmount0
        , b.uniAmount1
    from
    flaunch_prep1 a
    left join
    (
        select 
            evt_tx_hash
            , evt_block_date 
            , evt_block_number
            , evt_index + 1 as evt_index
            , uniAmount0
            , uniAmount1 
        from 
        flaunch_poolswap
    ) b
        on a.evt_tx_hash = b.evt_tx_hash
        and a.evt_block_date = b.evt_block_date 
        and a.evt_index = b.evt_index
    ) 
    group by 1, 2, 3, 5, 6, 7, 8 
),

get_trades as (
    select
        blockchain
        , project
        , version
        , block_month
        , block_date
        , block_time
        , date_trunc('minute', block_time) as block_minute 
        , block_number
        , token_bought_symbol
        , token_sold_symbol
        , token_pair
        , token_bought_amount
        , token_sold_amount
        , token_bought_amount_raw
        , token_sold_amount_raw
        , amount_usd
        , token_bought_address
        , token_sold_address
        , taker
        , maker
        , project_contract_address
        , tx_hash
        , tx_from
        , tx_to
        , evt_index
    from
    {{ ref('dex_trades') }}
    where block_date >= date '2025-08-01'
    {% if is_incremental() %}
    and {{ incremental_predicate('block_time') }}
    {% endif %}
    and project = 'uniswap'
),

add_fees as (
    select 
        gt.*
        , case 
            when gt.version = '4' then maker 
            else project_contract_address 
        end as pool_address 
        , coalesce (
            v4.fee/1e6
            , case 
                when gt.version = '2' then unp.fee/1e2 
                when gt.version = '3' then unp.fee/1e6 
            end -- v2 fees are set to 0.3 while v3 fees are the raw values
        ) as uni_fee 
        , coalesce (
            bf.fee/1e6
            , ff.fee 
        ) as hooks_fee 
        , coalesce (
            bf.hooks 
            , ff.hooks
        ) as hooks 
    from 
    get_trades gt 
    left join 
    v4_trades v4 
        on gt.blockchain = v4.blockchain 
        and gt.block_date = v4.block_date 
        and gt.tx_hash = v4.tx_hash 
        and gt.evt_index = v4.evt_index
        and gt.version = '4'
    left join 
    {{ ref('uniswap_pools') }} unp 
        on gt.blockchain = unp.blockchain
        and gt.project_contract_address = unp.pool 
        and gt.version = unp.version 
        and gt.version in ('2', '3')
    left join 
    bunni_fees bf 
        on gt.blockchain = bf.blockchain 
        and gt.block_date = bf.block_date
        and gt.tx_hash = bf.tx_hash
        and gt.evt_index = bf.evt_index
        and gt.maker = bf.id 
        and gt.version = '4'
    left join 
    univ4_base_trades uni_v4_base 
        on gt.blockchain = uni_v4_base.blockchain 
        and gt.block_date = uni_v4_base.evt_block_date
        and gt.tx_hash = uni_v4_base.evt_tx_hash 
        and gt.evt_index = uni_v4_base.evt_index 
        and gt.version = '4'
        and gt.blockchain = 'base'
    left join 
    flaunch_fees ff 
        on gt.blockchain = ff.blockchain 
        and gt.blockchain = 'base'
        and gt.block_date = ff.evt_block_date
        and gt.tx_hash = ff.evt_tx_hash
        and gt.maker = ff.id 
        and gt.version = '4'
        and uni_v4_base.amount0 = ff.uniAmount0 
        and uni_v4_base.amount1 = ff.uniAmount1
),

prices AS (
    select
        blockchain as price_blockchain
        , contract_address as price_contract_address
        , minute as price_minute
        , date_trunc('day', minute) as price_day 
        , price as price_usd 
    from
    {{ source('prices','usd_with_native') }}
    where minute >= date '2025-08-01'
    {% if is_incremental() %}
    and {{ incremental_predicate('minute') }}
    {% endif %}
    and blockchain != 'solana' -- filter solana tokens 
)

    select
        blockchain
        , project
        , version
        , block_month
        , block_date
        , block_time
        , block_number
        , token_bought_symbol
        , token_sold_symbol
        , token_pair
        , token_bought_amount * pb.price_usd as token_bought_amount_usd 
        , token_sold_amount * pa.price_usd as token_sold_amount_usd
        , token_bought_amount
        , token_sold_amount
        , token_bought_amount_raw
        , token_sold_amount_raw
        , amount_usd
        , token_bought_address
        , token_sold_address
        , taker
        , maker
        , project_contract_address
        , unp.pool_address 
        , unp.token0 as token0_address
        , unp.token1 as token1_address
        , case 
            when unp.token0 = token_bought_address then token_bought_symbol 
            else token_sold_symbol 
        end as token0_symbol 
        , case 
            when unp.token1 = token_bought_address then token_bought_symbol 
            else token_sold_symbol 
        end as token1_symbol 
        , tx_hash
        , tx_from
        , tx_to
        , evt_index
        -- uni fee columns 
        , coalesce (
            token_sold_amount * uni_fee * pa.price_usd
            , ((token_bought_amount * pb.price_usd) / (1 - uni_fee)) - (token_bought_amount * pb.price_usd)
            ) as lp_fee_amount_usd
        , token_sold_amount * uni_fee as lp_fee_amount 
        , token_sold_amount_raw * uni_fee as lp_fee_amount_raw
        , uni_fee * 1e2 as lp_fee -- convert back to correct value 
        -- hooks fee columns 
        , coalesce (
            token_sold_amount * hooks_fee * pa.price_usd
            , ((token_bought_amount * pb.price_usd) / (1 - hooks_fee)) - (token_bought_amount * pb.price_usd)
            ) as hooks_fee_amount_usd
        , token_sold_amount * hooks_fee as hooks_fee_amount 
        , token_sold_amount_raw * hooks_fee as hooks_fee_amount_raw
        , hooks_fee * 1e2 as hooks_fee -- convert back to correct value 
        , hooks 
    from 
    add_fees af 
    left join 
    prices pa 
        on af.block_date = pa.price_day 
        and af.block_minute = pa.price_minute 
        and af.blockchain = pa.price_blockchain 
        and af.token_sold_address = pa.price_contract_address
    left join 
    prices pb 
        on af.block_date = pb.price_day 
        and af.block_minute = pb.price_minute 
        and af.blockchain = pb.price_blockchain 
        and af.token_bought_address = pb.price_contract_address
    left join 
    {{ ref('uniswap_pools') }} unp 
        on af.blockchain = unp.blockchain
        and af.pool_address = unp.pool 
        and gt.version = unp.version 

