{{ config(
    alias = 'resolver_records',
    
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['node','block_time','evt_index'],
    post_hook='{{ expose_spells(\'["ethereum"]\',
                            "project",
                            "ens",
                            \'["0xRob, 0xr3x"]\') }}'
    )
}}

with resolver_records as (
    select
    a as address
    ,node
    ,evt_block_time as block_time
    ,evt_tx_hash as tx_hash
    ,evt_index
    from {{ source('ethereumnameservice_ethereum','PublicResolver_evt_AddrChanged') }}
    {% if is_incremental() %}
    WHERE evt_block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}
    UNION ALL   
    select
    a as address
    ,node
    ,evt_block_time as block_time
    ,evt_tx_hash as tx_hash
    ,evt_index
    from {{ source('ethereumnameservice_ethereum','PublicResolver_v2_evt_AddrChanged') }}
    {% if is_incremental() %}
    WHERE evt_block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}
   )

select
    n.name
    ,r.address
    ,r.node
    ,r.block_time
    ,r.tx_hash
    ,r.evt_index
from resolver_records r
inner join {{ ref('ens_node_names')}} n
ON r.node = n.node
