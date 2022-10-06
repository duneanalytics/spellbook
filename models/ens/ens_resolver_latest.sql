{{ config(
    alias = 'resolver_latest',
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

with latest_resolver_records as (
select
    node
    ,address
    ,block_time
    ,tx_hash
    from(
        select
        a as address
        ,node
        ,evt_block_time as block_time
        ,evt_tx_hash as tx_hash
        ,evt_index
        ,row_number() over (partition by node order by block_time desc, evt_index desc) as ordering
        from {{ source('ethereumnameservice_ethereum','PublicResolver_evt_AddrChanged') }}
        {% if is_incremental() %}
        WHERE evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
    ) foo
    where ordering = 1
   )

select
    ,n.name
    ,r.address
    ,r.node
    ,r.block_time
    ,r.tx_hash
from latest_resolver_records r
inner join {{ ref('ens_node_names')}} n
ON r.node = n.node
