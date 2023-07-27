{{ config(
	tags=['legacy'],
	
    alias = alias('resolver_latest', legacy_model=True),
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
    from {{ ref('ens_resolver_records_legacy')}}
) f
where ordering = 1
