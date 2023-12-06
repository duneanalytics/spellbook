{{ config(
    alias = 'node_names',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['node'],
    post_hook='{{ expose_spells(\'["ethereum"]\',
                            "project",
                            "ens",
                            \'["0xRob"]\') }}'
    )
}}

-- because we don't have the keccak namehash function available in v2
-- we do a little sketchy event matching to get the node <> name relationships
-- basically this takes the last AddrChanged event in the same tx preceding a NameRegistred event to link the node and the name
-- ONLY works for base ENS names (.eth , no subdomains)
with registrations as (
    select
        label as label_hash
        ,name as label_name
        ,evt_block_number as block_number
        ,evt_block_time as block_time
        ,evt_tx_hash as tx_hash
        ,evt_index
    from {{ ref('ens_view_registrations') }}
    {% if is_incremental() %}
    WHERE evt_block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}
)

,node_info as (
    select
        a as address
        ,node
        ,evt_block_number as block_number
        ,evt_block_time as block_time
        ,evt_tx_hash as tx_hash
        ,evt_index
    from {{ source('ethereumnameservice_ethereum','PublicResolver_evt_AddrChanged') }}
    {% if is_incremental() %}
    WHERE evt_block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}
    union 
    select
        a as address
        ,node
        ,evt_block_number as block_number
        ,evt_block_time as block_time
        ,evt_tx_hash as tx_hash
        ,evt_index
    from {{ source('ethereumnameservice_ethereum','PublicResolver_v2_evt_AddrChanged') }}
    {% if is_incremental() %}
    WHERE evt_block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}
)

-- here's the sketchy matching
, matching as (
    select *
    from (
        select *
        ,row_number() over (partition by node order by block_time desc, evt_index desc) as ordering2
        from (
            select
            r.*
            ,n.address
            ,n.node
            ,row_number() over (partition by r.tx_hash order by (r.evt_index - n.evt_index) asc) as ordering
            from registrations r
            inner join node_info n
            ON r.block_number = n.block_number
            AND r.tx_hash = n.tx_hash
            AND r.evt_index > n.evt_index --register event comes after node event
        )
        where ordering = 1
    )
    where ordering2 = 1
)

select
    node
    ,concat(label_name,'.eth') as name
    ,label_name
    ,label_hash
    ,address as initial_address
    ,tx_hash
    ,block_number
    ,block_time
    ,evt_index
from matching


