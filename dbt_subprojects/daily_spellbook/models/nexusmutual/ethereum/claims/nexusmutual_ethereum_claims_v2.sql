{{
  config(
    schema = 'nexusmutual_ethereum',
    alias = 'claims_v2',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['claim_id'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    post_hook = '{{ expose_spells(\'["ethereum"]\',
                                "project",
                                "nexusmutual",
                                \'["tomfutago"]\') }}'
  )
}}

with

claim_evt as (
  select
    evt_block_time as block_time,
    evt_block_number as block_number,
    evt_block_time as submit_time,
    date_trunc('day', evt_block_time) as submit_date,
    claimId as claim_id,
    coverId as cover_id,
    productId as product_id,
    user,
    evt_tx_hash as tx_hash
  from {{ source('nexusmutual_ethereum', 'IndividualClaims_evt_ClaimSubmitted') }}
  {% if is_incremental() %}
  where {{ incremental_predicate('evt_block_time') }}
  {% endif %}
),

claim_data as (
  select
    call_block_time as block_time,
    call_block_number as block_number,
    coverId as cover_id,
    cast(json_query(output_claim, 'lax $.assessmentId') as int) as assessment_id,
    cast(json_query(output_claim, 'lax $.coverAsset') as int) as cover_asset,
    cast(json_query(output_claim, 'lax $.payoutRedeemed') as boolean) as is_payout_redeemed,
    requestedAmount as requested_amount,
    ipfsMetadata as ipfs_metadata,
    call_tx_hash as tx_hash,
    row_number() over (partition by call_block_time, call_tx_hash, coverId order by call_trace_address desc) as rn
  from {{ source('nexusmutual_ethereum', 'IndividualClaims_call_submitClaimFor') }}
  where call_success
    {% if is_incremental() %}
    and {{ incremental_predicate('call_block_time') }}
    {% endif %}
  union all
  select
    call_block_time as block_time,
    call_block_number as block_number,
    coverId as cover_id,
    cast(json_query(output_claim, 'lax $.assessmentId') as int) as assessment_id,
    cast(json_query(output_claim, 'lax $.coverAsset') as int) as cover_asset,
    cast(json_query(output_claim, 'lax $.payoutRedeemed') as boolean) as is_payout_redeemed,
    requestedAmount,
    ipfsMetadata,
    call_tx_hash as tx_hash,
    row_number() over (partition by call_block_time, call_tx_hash, coverId order by call_trace_address desc) as rn
  from {{ source('nexusmutual_ethereum', 'IndividualClaims_call_submitClaim') }}
  where call_success
    {% if is_incremental() %}
    and {{ incremental_predicate('call_block_time') }}
    {% endif %}
)

select
  ce.block_time,
  ce.block_number,
  ce.submit_time,
  ce.submit_date,
  ce.claim_id,
  ce.cover_id,
  ce.product_id,
  ce.user,
  cd.assessment_id,
  case cd.cover_asset
    when 0 then 'ETH'
    when 1 then 'DAI'
    when 6 then 'USDC'
    else 'NA'
  end as cover_asset,
  cd.requested_amount / if(cd.cover_asset = 6, 1e6, 1e18) as requested_amount,
  cd.is_payout_redeemed,
  cd.ipfs_metadata,
  ce.tx_hash
from claim_evt ce
  inner join claim_data cd on ce.block_number = cd.block_number and ce.tx_hash = cd.tx_hash and ce.cover_id = cd.cover_id
where cd.rn = 1
