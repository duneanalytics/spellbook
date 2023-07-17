{{ config(
        alias = alias('punk_transfers'),
        partition_by = ['evt_block_time_week'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['evt_block_time_week', 'punk_id', 'evt_tx_hash', 'evt_index']
        )
}}

select *
from
(
        select  from
                , to
                , evt_block_time
                , evt_block_time_week
                , evt_block_number
                , evt_index
                , punk_id
                , evt_tx_hash
        from
        (   select  a.from
                , a.to
                , a.evt_block_time
                , date_trunc('week',a.evt_block_time) as evt_block_time_week
                , a.evt_block_number
                , a.evt_index
                , case when a.evt_tx_hash = lower('0x76d32b465ca332bbbe74f7a1834c6d354125f6950168c6123f8ab07440bc285e') and a.evt_index = 11 then 675
                        when a.evt_tx_hash = lower('0x76d32b465ca332bbbe74f7a1834c6d354125f6950168c6123f8ab07440bc285e') and a.evt_index = 27 then 675
                        when a.evt_tx_hash = lower('0x76d32b465ca332bbbe74f7a1834c6d354125f6950168c6123f8ab07440bc285e') and a.evt_index = 25 then 2138
                        when topic1 = '0x05af636b70da6819000c49f85b21fa82081c632069bb626f30932034099107d8' then cast(bytea2numeric_v3(substring(data from 3)) as int)
                        else cast(bytea2numeric_v3(substring(topic2 from 3)) as int) end as punk_id
                , a.evt_tx_hash
        from {{ source('erc20_ethereum','evt_transfer') }} a
        inner join {{ source('ethereum','logs') }} b
                        on a.evt_tx_hash = b.tx_hash
                        {% if is_incremental() %}
                        and b.block_time >= date_trunc('day', now() - interval '1 week')
                        {% endif %}
        where a.contract_address = lower('0xb47e3cd837dDF8e4c57F05d70Ab865de6e193BBB') -- cryptopunks contract
                and topic1 in   ( '0x58e5d5a525e3b40bc15abaa38b5882678db1ee68befd2f60bafe3a7fd06db9e3' -- PunkBought
                                , '0x05af636b70da6819000c49f85b21fa82081c632069bb626f30932034099107d8' -- PunkTransfer
                                )
                {% if is_incremental() %} and a.evt_block_time >= date_trunc('day', now() - interval '1 week') {% endif %}

        ) c
        group by 1,2,3,4,5,6,7,8

        union all

        select  '0x0000000000000000000000000000000000000000' as from
                , to
                , evt_block_time
                , date_trunc('week',evt_block_time) as evt_block_time_week
                , evt_block_number
                , evt_index
                , punkIndex as punk_id
                , evt_tx_hash
        from {{ source('cryptopunks_ethereum','CryptoPunksMarket_evt_Assign') }}
        {% if is_incremental() %} where evt_block_time >= date_trunc('day', now() - interval '1 week') {% endif %}

) d
order by evt_block_number desc, evt_index desc
