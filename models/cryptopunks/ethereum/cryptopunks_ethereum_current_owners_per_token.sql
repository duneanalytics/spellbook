{{ config(
        tags=['dunesql'],
        alias = alias('current_owners_per_token'),
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                    "project",
                                    "cryptopunks",
                                    \'["cat"]\') }}'
        )
}}

select punk_id
        , to as current_owner
        , evt_block_time as last_transfer_time
from 
(       select *
                , row_number() over (partition by punk_id order by evt_block_number desc, evt_index desc) as punk_id_tx_rank
        from  {{ ref('cryptopunks_ethereum_punk_transfers') }}
) a
where punk_id_tx_rank = 1 
order by punk_id asc
