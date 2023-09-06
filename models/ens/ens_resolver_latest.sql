{{ config(
    alias = alias('resolver_latest'),
    tags = ['dunesql'],
    post_hook='{{ expose_spells(\'["ethereum"]\',
                            "project",
                            "ens",
                            \'["0xRob"]\') }}'
    )
}}

select
    name
    ,address
    ,node
    ,block_time
    ,tx_hash
    ,evt_index
from(
     select
     *
    ,row_number() over (partition by node order by block_time desc, evt_index desc) as ordering
    from {{ ref('ens_resolver_records')}}
) f
where ordering = 1
