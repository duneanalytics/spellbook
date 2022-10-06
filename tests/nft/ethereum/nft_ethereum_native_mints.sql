-- Check if all ENS mints are taken into account
with
  ens_mints_ctn as (
    select
      count(*) as ctn,
      'hi_im_a_dummy' as dummy
    from
      ethereum.logs
    where
      1 = 1
      and contract_address = '0x57f1887a8bf19b14fc0df6fd9b2acc9af147ea85' -- contract = ENS
      and topic1 = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef' -- event type = transfer
      and topic2 = '0x0000000000000000000000000000000000000000000000000000000000000000' -- seller = null address
      and block_time < now() - interval '1 day' -- allow some head desync
	  
	  {% if is_incremental() %}
	  and block_time >= date_trunc("day", now() - interval '1 week')
	  {% endif %}
	  {% if not is_incremental() %}
	  and tx.block_number > 14801608
	  {% endif %}
  ),
  eth_native_mints_ctn as (
    select
      count(*) as ctn,
      'hi_im_a_dummy' as dummy
    from
      {{ ref('nft_ethereum_native_mints') }}
    where
      1 = 1
      and nft_contract_address = '0x57f1887a8bf19b14fc0df6fd9b2acc9af147ea85' -- ENS
      and block_time < now() - interval '1 day' -- allow some head desync
  )
select
  *
from
  ens_mints_ctn c1
  inner join eth_native_mints_ctn c2 on c1.dummy = c2.dummy
where
  -- pass test when difference in result rows is less than 0.01%
  (c1.ctn / c2.ctn) < 0.0001