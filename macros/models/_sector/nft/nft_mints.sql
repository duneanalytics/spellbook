{% macro nft_mints(
    blockchain,
    src_contracts,
    src_traces,
    src_transactions,
    src_prices_usd,
    src_erc20_evt_transfer,
    nft_transfers,
    nft_aggregators,
    tokens_nft,
    default_currency_symbol = 'ETH',
    default_currency_contract = 0x000000000000000000000000000000000000dead,
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

nfts_per_tx_tmp as (
  select
    tx_hash,
    sum(amount) as nfts_minted_in_tx
  from {{ nft_transfers }}
  {% if is_incremental () %}
  where {{ incremental_predicate('block_time') }}
  {% endif %}
  group by 1
),

nfts_per_tx as (
  select
    tx_hash,
    case
      when nfts_minted_in_tx = uint256 '0' then uint256 '1'
      else nfts_minted_in_tx
    end as nfts_minted_in_tx
  from nfts_per_tx_tmp
)

select
  blockchain,
  project,
  version,
  block_time,
  block_date,
  block_month,
  block_number,
  token_id,
  collection,
  token_standard,
  trade_type,
  number_of_items,
  trade_category,
  evt_type,
  seller,
  buyer,
  amount_raw,
  amount_original,
  amount_usd,
  currency_symbol,
  currency_contract,
  nft_contract_address,
  project_contract_address,
  aggregator_name,
  aggregator_address,
  tx_hash,
  tx_from,
  tx_to,
  platform_fee_amount_raw,
  platform_fee_amount,
  platform_fee_amount_usd,
  platform_fee_percentage,
  royalty_fee_receive_address,
  royalty_fee_currency_symbol,
  royalty_fee_amount_raw,
  royalty_fee_amount,
  royalty_fee_amount_usd,
  royalty_fee_percentage,
  evt_index
from (
    select
      *,
      row_number() over (
        partition by tx_hash, evt_index, token_id, number_of_items
        order by amount_usd desc nulls last
      ) as rank_index
    from (
        select distinct
          '{{blockchain}}' as blockchain,
          coalesce(ec.namespace, 'Unknown') as project,
          '' as version,
          nft_mints.block_time as block_time,
          cast(date_trunc('day', nft_mints.block_time) as date) as block_date,
          cast(date_trunc('month', nft_mints.block_time) as date) as block_month,
          nft_mints.block_number as block_number,
          nft_mints.token_id as token_id,
          tok.name as collection,
          nft_mints.token_standard,
          case
            when nft_mints.amount = uint256 '1' then 'Single Item Mint'
            else 'Bundle Mint'
          end as trade_type,
          nft_mints.amount as number_of_items,
          'Mint' as trade_category,
          'Mint' as evt_type,
          nft_mints."from" as seller,
          nft_mints.to as buyer,
          cast(
            coalesce(
              sum(cast(trc.value as double)),
              sum(cast(erc20s.value as double)),
              0
            ) * (nft_mints.amount / nft_count.nfts_minted_in_tx) as uint256
          ) as amount_raw,
          coalesce(
            sum(cast(trc.value as double)) / power(10, 18),
            sum(cast(erc20s.value as double)) / power(10, pu_erc20.decimals)
          ) * (nft_mints.amount / nft_count.nfts_minted_in_tx) as amount_original,
          coalesce(
            pu_native.price * sum(cast(trc.value as double)) / power(10, 18),
            pu_erc20.price * sum(cast(erc20s.value as double)) / power(10, pu_erc20.decimals)
          ) * (nft_mints.amount / nft_count.nfts_minted_in_tx) as amount_usd,
          case
            when trc.success then '{{ default_currency_symbol }}'
            else pu_erc20.symbol
          end as currency_symbol,
          case
            when trc.success then {{ default_currency_contract }}
            else erc20s.contract_address
          end as currency_contract,
          nft_mints.contract_address as nft_contract_address,
          tx.to as project_contract_address,
          agg.name as aggregator_name,
          agg.contract_address as aggregator_address,
          nft_mints.tx_hash as tx_hash,
          tx."from" as tx_from,
          tx.to as tx_to,
          cast(0 as uint256) as platform_fee_amount_raw,
          cast(0 as double) as platform_fee_amount,
          cast(0 as double) as platform_fee_amount_usd,
          cast(0 as double) as platform_fee_percentage,
          cast(null as varbinary) as royalty_fee_receive_address,
          '0' as royalty_fee_currency_symbol,
          cast(0 as uint256) as royalty_fee_amount_raw,
          cast(0 as double) as royalty_fee_amount,
          cast(0 as double) as royalty_fee_amount_usd,
          cast(0 as double) as royalty_fee_percentage,
          nft_mints.evt_index
        from {{ nft_transfers }} nft_mints
          left join nfts_per_tx nft_count on nft_count.tx_hash = nft_mints.tx_hash
          left join {{ src_traces }} trc on trc.block_time = nft_mints.block_time
            and trc.tx_hash = nft_mints.tx_hash
            and trc."from" = nft_mints.to
            and (trc.call_type not in ('delegatecall', 'callcode', 'staticcall') or trc.call_type is null)
            and trc.success
            and cast(trc.value as double) > 0
            {% if is_incremental () %}
            and {{ incremental_predicate('trc.block_time') }}
            {% endif %}
          left join {{ src_prices_usd }} pu_native on pu_native.blockchain = '{{blockchain}}'
            and pu_native.minute = date_trunc('minute', trc.block_time)
            and pu_native.contract_address = 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2
            {% if is_incremental () %}
            and {{ incremental_predicate('pu_native.minute') }}
            {% endif %}
          left join {{ src_erc20_evt_transfer }} erc20s on erc20s.evt_block_time = nft_mints.block_time
            and erc20s."from" = nft_mints.to
            and erc20s.evt_tx_hash = nft_mints.tx_hash
            and (trc.value is null or cast(trc.value as double) = 0)
            {% if is_incremental () %}
            and {{ incremental_predicate('erc20s.evt_block_time') }}
            {% endif %}
          left join {{ src_prices_usd }} pu_erc20 on pu_erc20.blockchain = '{{blockchain}}'
            and pu_erc20.minute = date_trunc('minute', erc20s.evt_block_time)
            and erc20s.contract_address = pu_erc20.contract_address
            {% if is_incremental () %}
            and {{ incremental_predicate('pu_erc20.minute') }}
            {% endif %}
          left join {{ src_transactions }} tx on tx.block_time = nft_mints.block_time
            and tx.hash = nft_mints.tx_hash
            {% if is_incremental () %}
            and {{ incremental_predicate('tx.block_time') }}
            {% endif %}
          left join {{ nft_aggregators }} agg on tx.to = agg.contract_address
          left join {{ tokens_nft }} tok on tok.contract_address = nft_mints.contract_address
          left join namespaces ec on tx.to = ec.address
        where 1 = 1
          and nft_mints."from" = 0x0000000000000000000000000000000000000000
          {% if addresses_defi %}
          and nft_mints.contract_address not in (select address from {{ addresses_defi }})
          {% endif %}
          {% if is_incremental () %}
          and {{ incremental_predicate('nft_mints.block_time') }}
          {% endif %}
        group by
          nft_mints.block_time,
          nft_mints.block_number,
          nft_mints.token_id,
          nft_mints.token_standard,
          nft_mints.amount,
          nft_mints."from",
          nft_mints.to,
          nft_mints.contract_address,
          tx.to,
          nft_mints.evt_index,
          nft_mints.tx_hash,
          tx."from",
          ec.namespace,
          tok.name,
          pu_erc20.decimals,
          pu_native.price,
          pu_erc20.price,
          agg.name,
          agg.contract_address,
          nft_count.nfts_minted_in_tx,
          pu_erc20.symbol,
          erc20s.contract_address,
          trc.success
      ) tmp
  ) tmp_2
where rank_index = 1

{% endmacro %}
