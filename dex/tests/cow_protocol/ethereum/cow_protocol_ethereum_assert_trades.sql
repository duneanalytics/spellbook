-- Try it out here: https://dune.com/queries/1398185
select evt_tx_hash
from {{ source('gnosis_protocol_v2_ethereum', 'GPv2Settlement_evt_Trade') }}
where evt_tx_hash not in (select tx_hash from {{ ref('cow_protocol_ethereum_batches')}})
and evt_block_time < date(now()) - interval '1' day