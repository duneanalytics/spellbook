-- Check if all BSC Tofu trade events make it into the nft.trades
WITH tff AS (
    SELECT call_block_time,
           call_tx_hash,
           get_json_object(t, '$.token')   as token,
           get_json_object(t, '$.tokenId') as token_id
    FROM (SELECT call_block_time,
                 call_tx_hash,
                 explode(from_json(get_json_object(detail, '$.bundle'), 'array<string>')) as t
          FROM {{ source('tofu_nft_bnb', 'MarketNG_call_run') }}
          WHERE call_success = true
            and call_block_time >= '2022-12-01' -- Check from Dec 1 2022
            and call_block_time
              < NOW() - interval '1 day'
         ) as tmp
),
     tfe as (
         select evt_tx_hash,
                evt_block_time,
                get_json_object(inventory, '$.seller')   as seller,
                get_json_object(inventory, '$.buyer')    as buyer,
                get_json_object(inventory, '$.kind')     as kind,
                get_json_object(inventory, '$.price')    as price,
                get_json_object(inventory, '$.currency') as currency
         from {{ source('tofu_nft_bnb', 'MarketNG_evt_EvInventoryUpdate') }}
where get_json_object(inventory, '$.status') = '1'
  and evt_block_time >= '2022-12-01'
  and evt_block_time < NOW() - interval '1 day'
    )
    , raw_events as (
select tfe.evt_block_time as raw_block_time
        , tfe.evt_tx_hash as raw_tx_hash
        , tff.token as raw_nft_contract_address
        , tff.token_id as raw_token_id
        , tfe.evt_tx_hash || '-' || tff.token || '-' || tff.token_id AS raw_unique_trade_id
from tfe
    join tff
on tfe.evt_tx_hash = tff.call_tx_hash
    AND tff.call_block_time = tfe.evt_block_time
    ),

    processed_events as (
SELECT block_time AS processed_block_time
        , tx_hash AS processed_tx_hash
        , nft_contract_address AS processed_nft_contract_address
        , token_id AS processed_token_id
        , tx_hash || '-' || nft_contract_address || '-' || token_id AS processed_trade_id
FROM {{ ref('tofu_bnb_events') }}
WHERE blockchain = 'bnb'
  AND project = 'tofu'
  AND version = 'v1'
  AND block_time >= '2022-12-01'
  AND block_time
    < NOW() - interval '1 day'
    )

select *
from raw_events as r
full join processed_events as p
on r.raw_block_time = p.processed_block_time
    and r.raw_unique_trade_id = p.processed_trade_id
where not r.raw_unique_trade_id = p.processed_trade_id
