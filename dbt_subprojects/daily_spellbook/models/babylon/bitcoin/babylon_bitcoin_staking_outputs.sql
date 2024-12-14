{{ config(
        schema = 'babylon_btc',
        alias = 'staking_outputs',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['tx_id'],
        post_hook='{{ expose_spells(\'["bitcoin"]\',
                                "project",
                                "babylon",
                                \'["maybeYonas", "pyor_xyz"]\') }}'
    )
}}

with
latest_block as (
    select max(block_height) as max_block 
    from bitcoin.transactions
),
{% if is_incremental() %}
max_timestamp as (
    select max(block_time) as max_block_time
    from {{this}}
),
{% endif %}
outputs as (
    select 
        o.block_time,
        o.block_height,
        o.tx_id,
        o.value,
        o.address,
        o.type,
        o.script_hex
    from bitcoin.outputs 0
    where block_time > date'2024-08-22'
    and block_height >= 857909
    {% if is_incremental() %}
        and block_time > (select max_block_time from max_timestamp)

    union all
    select
        o.block_time,
        o.block_height,
        o.tx_id,
        o.value,
        o.address,
        o.type,
        o.script_hex
    from {{this}} o
    where unstake_tx_id is null
    {% endif %}
),
inputs as (
    select 
        i.tx_id,
        i.spent_tx_id,
        i.block_time
    from bitcoin.inputs i
    where block_time > date'2024-08-22'
    and block_height >= 857909
    {% if is_incremental() %}
        and block_time > (select max_block_time from max_timestamp)
    {% endif %}
),
transactions as ( 
    select id,index
    from bitcoin.transactions
    where block_time > date'2024-08-22'
    and block_height >= 857909
    {% if is_incremental() %}
        and block_time > (select max_block_time from max_timestamp)

    union all
    select
        i.tx_id as id,
        i.index
    from {{this}} i
    where unstake_tx_id is null
    {% endif %}
),
restaking_txs as (
    select 
        o.tx_id,
        
        substr(o.script_hex,7,1) as version,
        substr(o.script_hex,8,32) as staker,
        substr(o.script_hex,40,32) as finality_provider,
        substr(o.script_hex,72,2) as stakingtime,
        
        t.index
        -- stakingtime()
    from outputs o
        join transactions t 
            on o.tx_id = t.id
    -- and block_height = 853843
    -- and tx_id = 0x45f9cbcc4b4a5b58feef62e8462199c6079d42e24130f155594c9af8904e0f0d
    where substr(script_hex, 1,6) = 0x6a4762626e31
),
restake_info as (
    select 
        o.block_time,
        o.block_height,
        o.tx_id,
        o.value,
        o.address,
        o.type,
        r.version,
        r.staker,
        r.finality_provider,
        r.stakingtime,
        r.index,
        d.data__description__moniker as finality_provider_name,
        
        i.tx_id as unstake_tx_id,
        i.block_time as unstake_block_time,
        o.script_hex
    from outputs o
        join restaking_txs r 
            on o.tx_id = r.tx_id
        left join {{ref('babylon_bitcoin_finality_providers')}} d
            on r.finality_provider = from_hex('0x'||d.data__btc_pk)
        left join inputs i 
            on o.tx_id = i.spent_tx_id
            and o.address = i.address
    where o.index = 0
),
data as (
    select i.*,
        sum(value) over(order by block_height,index) as total_btc,
        case 
            when i.block_height < l.max_block-6 
            then 'confirmed' 
            else 'pending' 
        end as status
    from restake_info i,
        latest_block l
)

select 
    block_time,
    block_height,
    tx_id,
    value,
    address,
    type,
    version,
    staker,
    finality_provider,
    stakingtime,
    index,
    finality_provider_name,
    unstake_tx_id,
    unstake_block_time,
    total_btc,
    status,
    case when 
        total_btc - value < 1000 
        or
        (block_height >= 864790 and block_height <= 864799)
        or
        (block_height >= 874088 and block_height <= 875087)
    then 'accepted'
    else 'overflow' 
    end as babylon_acceptance,
    case 
        when unstake_tx_id is not null 
        then 'unstaked' 
        else 'staked' 
    end as babylon_status
    script_hex
from data