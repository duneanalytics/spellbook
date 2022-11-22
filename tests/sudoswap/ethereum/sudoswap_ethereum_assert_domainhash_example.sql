-- example from https://twitter.com/vasa_develop/status/1579830106067202049?s=20&t=GgkgSf__TCLuq2rjDjAajg
-- https://etherscan.io/tx/0xa017a142978ecd8a514e1e8337aedd91507a6646fdc403e975ebab7ccb4fba7b

select *
from {{ ref('sudoswap_ethereum_events') }}
where block_number = 15725191 and tx_hash = '0xa017a142978ecd8a514e1e8337aedd91507a6646fdc403e975ebab7ccb4fba7b'
AND aggregator_name != 'Gem'
