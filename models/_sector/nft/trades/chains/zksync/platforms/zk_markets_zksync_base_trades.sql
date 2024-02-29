{{
    config(
        schema = 'zk_markets_zksync',
        alias = 'base_trades',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['block_number','tx_hash','sub_tx_trade_id'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    )
}}

with

base_trades_combined as (
    select
      eb.evt_block_time as block_time,
      eb.evt_block_number as block_number,
      eb.nftAddress as nft_contract_address,
      eb.tokenId as nft_token_id,
      uint256 '1' as nft_amount,
      eb.seller,
      eb.buyer,
      eb.price as price_raw,
      eb.contract_address as project_contract_address,
      eb.evt_tx_hash as tx_hash,
      eb.evt_index
    from {{ source('zk_markets_zksync', 'AANFTMarketplace_evt_ItemBought') }} eb
    {% if is_incremental() %}
    where {{incremental_predicate('eb.evt_block_time')}}
    {% endif %}

    union all

    select
      eb.evt_block_time as block_time,
      eb.evt_block_number as block_number,
      eb.nftAddress as nft_contract_address,
      eb.tokenId as nft_token_id,
      uint256 '1' as nft_amount,
      eb.seller,
      eb.buyer,
      eb.price as price_raw,
      eb.contract_address as project_contract_address,
      eb.evt_tx_hash as tx_hash,
      eb.evt_index
    from {{ source('zk_markets_zksync', 'NftMarketplace_evt_ItemBought') }} eb
    {% if is_incremental() %}
    where {{incremental_predicate('eb.evt_block_time')}}
    {% endif %}

    union all

    select
      cb.call_block_time as block_time,
      cb.call_block_number as block_number,
      cb.nftAddress as nft_contract_address,
      cb.tokenId as nft_token_id,
      uint256 '1' as nft_amount,
      nft."from" as seller,
      nft.to as buyer,
      tx.value as price_raw,
      cb.contract_address as project_contract_address,
      cb.call_tx_hash as tx_hash,
      tx.index as evt_index
    from {{ source('zk_markets_zksync', 'AANftMarketplace_call_buyItem') }} cb
      join {{ ref('nft_zksync_transfers') }} nft
        on cb.nftAddress = nft.contract_address
       and cb.tokenId = nft.token_id
       and cb.call_tx_hash = nft.tx_hash
      join {{ source('zksync', 'transactions') }} tx
        on cb.call_block_number = tx.block_number
       and cb.call_tx_hash = tx.hash
    where cb.call_success
      and tx.success
      {% if is_incremental() %}
      and {{incremental_predicate('cb.call_block_time')}}
      and {{incremental_predicate('nft.block_time')}}
      and {{incremental_predicate('tx.block_time')}}
      {% endif %}
),

royalties as (
    with transfers as (
      select
        tr.block_number,
        tr.block_time,
        tr.tx_hash,
        tr.amount_raw as value,
        tr.to,
        tc.evt_index,
        tc.evt_index - coalesce(tr.evt_index, element_at(tr.trace_address, 1), 0) as ranking
      from base_trades_combined as tc
        join {{ ref('tokens_zksync_base_transfers') }} as tr
          on tc.block_number = tr.block_number
         and tc.tx_hash = tr.tx_hash
      where tr.amount_raw > 0
        and tr."from" in (tc.project_contract_address, tc.buyer) -- only include transfer from marketplace or buyer
        and tr.to not in (
          tc.project_contract_address,
          tc.seller,
          0x0000000000000000000000000000000000008001 -- system contract
        )
        {% if is_incremental() %}
        and {{incremental_predicate('tr.block_time')}}
        {% endif %}
    )
    select
      t.block_number,
      t.block_time,
      t.tx_hash,
      t.value,
      t.to,
      t.evt_index
    from (
        select *, row_number() over (partition by tx_hash, evt_index order by abs(ranking)) as rn
        from transfers
      ) as t
    where t.rn = 1 -- select closest by order
),

base_trades as (
    select
      'zksync' as blockchain,
      'zk_markets' as project,
      '1' as project_version,
      t.block_time,
      cast(date_trunc('day', t.block_time) as date) as block_date,
      cast(date_trunc('month', t.block_time) as date) as block_month,
      t.block_number,
      t.nft_contract_address,
      t.nft_token_id,
      t.nft_amount,
      t.seller,
      t.buyer,
      'Buy' as trade_category,
      'secondary' as trade_type,
      t.price_raw,
      0x000000000000000000000000000000000000800a as currency_contract, -- ETH
      t.project_contract_address,
      t.tx_hash,
      cast(null as uint256) as platform_fee_amount_raw,
      r.value as royalty_fee_amount_raw,
      r.to as royalty_fee_address,
      cast(null as varbinary) as platform_fee_address,
      t.evt_index as sub_tx_trade_id
    from base_trades_combined t
      left join royalties r
        on t.block_number = r.block_number
       and t.tx_hash = r.tx_hash
       and t.evt_index = r.evt_index
)

-- this will be removed once tx_from and tx_to are available in the base event tables
{{ add_nft_tx_data('base_trades', 'zksync') }}
