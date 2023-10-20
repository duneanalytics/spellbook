{% macro nft_mints(
    blockchain,
    src_contracts,
    src_traces,
    src_transactions,
    src_erc20_evt_transfer,
    nft_transfers,
    nft_aggregators,
    tokens_nft,
    default_currency_symbol = 'ETH',
    default_currency_contract = '0x000000000000000000000000000000000000dead',
    addresses_defi = null
)%}

with

namespaces as (
  select
    address,
    min_by(namespace, created_at) as namespace
  from {{ src_contracts }}
  group by 1
),

nfts_per_tx as (
  select
    tx_hash,
    sum(cast(amount as double)) as nfts_minted_in_tx -- for some erc1155 uint256 is not enough
  from {{ nft_transfers }}
  where "from" = 0x0000000000000000000000000000000000000000
    {% if is_incremental () %}
    and {{ incremental_predicate('block_time') }}
    {% endif %}
  group by 1
),

nft_mints as (
  select
    nft_mints.block_time as block_time,
    cast(date_trunc('day', nft_mints.block_time) as date) as block_date,
    cast(date_trunc('month', nft_mints.block_time) as date) as block_month,
    nft_mints.block_number,
    nft_mints.token_id,
    nft_mints.token_standard,
    case
      when nft_mints.amount = uint256 '1' then 'Single Item Mint'
      else 'Bundle Mint'
    end as trade_type,
    nft_mints.amount as number_of_items,
    'Mint' as trade_category,
    'Mint' as evt_type,
    nft_mints."from" as seller, -- needed? always burn address
    nft_mints.to as buyer,
    nft_mints.amount / case when nft_count.nfts_minted_in_tx = 0 then 1 else nft_count.nfts_minted_in_tx end as mint_ratio,
    nft_mints.contract_address as nft_contract_address,
    nft_mints.tx_hash,
    nft_mints.evt_index,
    tx.to as project_contract_address,
    tx."from" as tx_from,
    tx.to as tx_to
  from {{ nft_transfers }} nft_mints
    join nfts_per_tx nft_count on nft_count.tx_hash = nft_mints.tx_hash
    join {{ src_transactions }} tx on nft_mints.block_time = tx.block_time and nft_mints.tx_hash = tx.hash
      {% if is_incremental () %}
      and {{ incremental_predicate('tx.block_time') }}
      {% endif %}
  where 1 = 1
    and nft_mints."from" = 0x0000000000000000000000000000000000000000
    {% if is_incremental () %}
    and {{ incremental_predicate('nft_mints.block_time') }}
    {% endif %}
),

nft_mint_with_native as (
  select 
    nft_mints.block_time,
    nft_mints.tx_hash,
    nft_mints.token_id,
    {{ default_currency_contract }} as contract_address,
    '{{ default_currency_symbol }}' as symbol,
    cast(sum(coalesce(cast(trc.value as double), 0) * nft_mints.mint_ratio) as uint256) as amount_raw,
    sum((coalesce(cast(trc.value as double), 0) / power(10, 18)) * nft_mints.mint_ratio) as amount_original,
    sum((coalesce(cast(trc.value as double), 0) / power(10, 18)) * nft_mints.mint_ratio * pu_native.price) as amount_usd
  from nft_mints
    join {{ src_traces }} trc on nft_mints.block_time = trc.block_time
      and nft_mints.tx_hash = trc.tx_hash
      and nft_mints.buyer = trc."from"
      {% if is_incremental () %}
      and {{ incremental_predicate('trc.block_time') }}
      {% endif %}
    left join {{ source('prices', 'usd') }} pu_native on pu_native.blockchain = '{{blockchain}}'
      and pu_native.minute = date_trunc('minute', trc.block_time)
      and pu_native.contract_address = {{ default_currency_contract }}
      {% if is_incremental () %}
      and {{ incremental_predicate('pu_native.minute') }}
      {% endif %}
  where 1 = 1
    and (trc.call_type not in ('delegatecall', 'callcode', 'staticcall') or trc.call_type is null)
    and cast(trc.value as double) > 0
    and trc.success
  group by 1,2,3
),

nft_mint_with_erc20 as (
  select 
    nft_mints.block_time,
    nft_mints.tx_hash,
    nft_mints.token_id,
    erc20.contract_address,
    pu_erc20.symbol,
    cast(sum(coalesce(cast(erc20.value as double), 0) * nft_mints.mint_ratio) as uint256) as amount_raw, -- sum? what if 2+ different tokens? like when Uniswap V3 Positions NFT-V1 is minted
    sum((coalesce(cast(erc20.value as double), 0) / power(10, 18)) * nft_mints.mint_ratio) as amount_original,
    sum((coalesce(cast(erc20.value as double), 0) / power(10, 18)) * nft_mints.mint_ratio * pu_erc20.price) as amount_usd
  from nft_mints
    join {{ src_erc20_evt_transfer }} erc20 on nft_mints.block_time = erc20.evt_block_time
      and nft_mints.tx_hash = erc20.evt_tx_hash
      and nft_mints.buyer = erc20."from"
      {% if is_incremental () %}
      and {{ incremental_predicate('erc20.evt_block_time') }}
      {% endif %}
    left join {{ source('prices', 'usd') }} pu_erc20 on pu_erc20.blockchain = '{{blockchain}}'
      and pu_erc20.minute = date_trunc('minute', erc20.evt_block_time)
      and erc20.contract_address = pu_erc20.contract_address
      {% if is_incremental () %}
      and {{ incremental_predicate('pu_erc20.minute') }}
      {% endif %}
  where 1 = 1
    and cast(erc20.value as double) > 0
  group by 1,2,3,4,5
)

select
  '{{blockchain}}' as blockchain,
  coalesce(ec.namespace, 'Unknown') as project,
  coalesce(tok.name, 'Unknown') as collection,
  '' as version,
  nft_mints.block_time,
  nft_mints.block_date,
  nft_mints.block_month,
  nft_mints.block_number,
  nft_mints.token_id,
  nft_mints.token_standard,
  nft_mints.trade_type,
  nft_mints.number_of_items,
  nft_mints.trade_category,
  nft_mints.evt_type,
  nft_mints.seller,
  nft_mints.buyer,
  nft_mints.nft_contract_address,
  nft_mints.tx_hash,
  nft_mints.evt_index,
  nft_mints.project_contract_address,
  nft_mints.tx_from,
  nft_mints.tx_to,
  coalesce(mint_erc20.amount_raw, mint_native.amount_raw, uint256 '0') as amount_raw,
  coalesce(mint_erc20.amount_original, mint_native.amount_original, 0) as amount_original,
  coalesce(mint_erc20.amount_usd, mint_native.amount_usd, 0) as amount_usd,
  coalesce(mint_erc20.symbol, mint_native.symbol, '{{ default_currency_symbol }}') as currency_symbol,
  coalesce(mint_erc20.contract_address, mint_native.contract_address, {{ default_currency_contract }}) as currency_contract,
  agg.name as aggregator_name,
  agg.contract_address as aggregator_address,
  cast(0 as uint256) as platform_fee_amount_raw,
  cast(0 as double) as platform_fee_amount,
  cast(0 as double) as platform_fee_amount_usd,
  cast(0 as double) as platform_fee_percentage,
  cast(null as varbinary) as royalty_fee_receive_address,
  '0' as royalty_fee_currency_symbol,
  cast(0 as uint256) as royalty_fee_amount_raw,
  cast(0 as double) as royalty_fee_amount,
  cast(0 as double) as royalty_fee_amount_usd,
  cast(0 as double) as royalty_fee_percentage
from nft_mints
  left join nft_mint_with_native mint_native on nft_mints.block_time = mint_native.block_time
    and nft_mints.tx_hash = mint_native.tx_hash
    and nft_mints.token_id = mint_native.token_id
  left join nft_mint_with_erc20 mint_erc20 on nft_mints.block_time = mint_erc20.block_time
    and nft_mints.tx_hash = mint_erc20.tx_hash
    and nft_mints.token_id = mint_erc20.token_id
  left join {{ nft_aggregators }} agg on nft_mints.tx_to = agg.contract_address
  left join {{ tokens_nft }} tok on nft_mints.nft_contract_address = tok.contract_address
  left join namespaces ec on nft_mints.tx_to = ec.address
where 1 = 1
  {% if addresses_defi %}
  and nft_mints.contract_address not in (select address from {{ addresses_defi }})
  {% endif %}
  {% if is_incremental () %}
  and {{ incremental_predicate('nft_mints.block_time') }}
  {% endif %}

{% endmacro %}
