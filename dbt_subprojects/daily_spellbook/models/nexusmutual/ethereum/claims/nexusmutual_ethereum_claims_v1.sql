{{
  config(
    schema = 'nexusmutual_ethereum',
    alias = 'claims_v1',
    materialized = 'view',
    unique_key = ['claim_id'],
    post_hook = '{{ expose_spells(\'["ethereum"]\',
                                "project",
                                "nexusmutual",
                                \'["tomfutago"]\') }}'
  )
}}

with

claims as (
  select
    cr.evt_block_time as block_time,
    cr.evt_block_number as block_number,
    cr.claimId as claim_id,
    cr.coverId as cover_id,
    cr.userAddress as claimant,
    if(cr.claimId = 102, timestamp '2021-11-05', from_unixtime(cr.dateSubmit)) as submit_time,
    if(cr.claimId = 102, cast(10.43 as double), cast(cp.requestedPayoutAmount as double)) as partial_claim_amount,
    cr.evt_tx_hash as tx_hash
  from {{ source('nexusmutual_ethereum', 'ClaimsData_evt_ClaimRaise') }} cr
    left join {{ source('nexusmutual_ethereum', 'Claims_call_submitPartialClaim') }} cp on cr.coverId = cp.coverId
      and cr.evt_tx_hash = cp.call_tx_hash
      and cp.requestedPayoutAmount > 0
      and cp.call_success
)

select
  block_time,
  block_number,
  submit_time,
  submit_date,
  claim_id,
  cover_id,
  claimant,
  partial_claim_amount,
  claim_status,
  tx_hash
from (
  select
    c.block_time,
    c.block_number,
    c.claim_id,
    c.cover_id,
    c.claimant,
    c.submit_time,
    date_trunc('day', c.submit_time) as submit_date,
    c.partial_claim_amount,
    cs._stat as claim_status,
    c.tx_hash,
    row_number() over (partition by c.claim_id order by cs._stat desc) as rn
  from {{ source('nexusmutual_ethereum', 'ClaimsData_call_setClaimStatus') }} cs
    inner join claims c on cs._claimId = c.claim_id
  where cs.call_success
) t
where rn = 1
