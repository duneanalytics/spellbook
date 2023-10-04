{{ config(
	tags=['legacy'],
	    schema = 'nft_optimism',
        alias = alias('native_mints', legacy_model=True),
		materialized = 'incremental',
		file_format = 'delta',
		incremental_strategy = 'merge',
        unique_key='unique_trade_id',
        post_hook='{{ expose_spells(\'["optimism"]\',
                                    "sector",
                                    "nft",
                                    \'["chuxin"]\') }}')
}}
{% set eth_address = "0xdeaddeaddeaddeaddeaddeaddeaddeaddead0000" %}

with namespaces as (
    select
        contract_address as address
        ,coalesce(contract_project, contract_name, token_symbol) as namespace
	from {{ ref('contracts_optimism_contract_mapping_legacy') }}
)
, nfts_per_tx as (
    select
        tx_hash
        ,sum(amount) as nfts_minted_in_tx
        from {{ ref('nft_optimism_transfers_legacy') }}
        {% if is_incremental() %}
        where block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
    group by 1
)
select
    'optimism' as blockchain
    , coalesce(lower(ec.namespace), 'Unknown') as project
    , '' as version
    , nft_mints.block_time as block_time
    , date_trunc('day', nft_mints.block_time) as block_date
    , nft_mints.block_number as block_number
    , nft_mints.token_id as token_id
    , tok.name as collection
    , nft_mints.token_standard
    , case
        when nft_mints.amount=1 then 'Single Item Mint'
        else 'Bundle Mint'
        end as trade_type
    , cast(nft_mints.amount as decimal(38,0)) as number_of_items
    , 'Mint' as trade_category
    , 'Mint' as evt_type
    , nft_mints.from as seller
    , nft_mints.to as buyer
    , case when tr.tx_hash is not null then 'ETH' else pu_erc20s.symbol end as currency_symbol
    , case when tr.tx_hash is not null then '{{eth_address}}' else erc20s.contract_address end as currency_contract
    , nft_mints.contract_address as nft_contract_address
    , etxs.to as project_contract_address
    , agg.name as aggregator_name
    , agg.contract_address as aggregator_address
    , nft_mints.tx_hash as tx_hash
    , etxs.from as tx_from
    , etxs.to as tx_to
    , cast(0 as double) as platform_fee_amount_raw
    , cast(0 as double) as platform_fee_amount
    , cast(0 as double) as platform_fee_amount_usd
    , cast(0 as double) as platform_fee_percentage
    , '' as royalty_fee_receive_address
    , cast('0' as varchar(5)) as royalty_fee_currency_symbol
    , cast(0 as double) as royalty_fee_amount_raw
    , cast(0 as double) as royalty_fee_amount
    , cast(0 as double) as royalty_fee_amount_usd
    , cast(0 as double) as royalty_fee_percentage
    , 'optimism' || '-' || coalesce(ec.namespace, 'Unknown') || '-Mint-' || coalesce(nft_mints.tx_hash, '-1') || '-' || coalesce(nft_mints.to, '-1') || '-' ||  coalesce(nft_mints.contract_address, '-1') || '-' || coalesce(nft_mints.token_id, '-1') || '-' || coalesce(nft_mints.amount, '-1') || '-'|| coalesce(erc20s.contract_address, '0x0000000000000000000000000000000000000000') || '-' || coalesce(nft_mints.evt_index, '-1') as unique_trade_id
    , cast(coalesce(sum(tr.value), sum(cast(erc20s.value as double)), 0)*(nft_mints.amount/nft_count.nfts_minted_in_tx) as decimal(38,0)) as amount_raw
    , coalesce(sum(tr.value_decimal), sum(cast(erc20s.value as double))/power(10, pu_erc20s.decimals))*(nft_mints.amount/nft_count.nfts_minted_in_tx) as amount_original
    , coalesce(pu_eth.price*sum(tr.value_decimal), pu_erc20s.price*sum(cast(erc20s.value as double))/power(10, pu_erc20s.decimals))*(nft_mints.amount/nft_count.nfts_minted_in_tx) as amount_usd
from {{ ref('nft_optimism_transfers_legacy') }} as nft_mints
left join {{ source('optimism','transactions') }} as etxs
    on etxs.block_time=nft_mints.block_time
    and etxs.hash=nft_mints.tx_hash
    {% if is_incremental() %}
    and etxs.block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}
left join {{ ref('tokens_optimism_nft_legacy') }} as tok
    on tok.contract_address=nft_mints.contract_address
left join {{ ref('tokens_optimism_nft_bridged_mapping_legacy') }} as bm
    on bm.contract_address=nft_mints.contract_address
left join {{ ref('transfers_optimism_eth_legacy') }} as tr
    on nft_mints.tx_hash = tr.tx_hash
    and nft_mints.block_number = tr.tx_block_number
    and tr.value_decimal > 0
left join {{ source('prices','usd') }} as pu_eth
    on pu_eth.blockchain='optimism'
    and pu_eth.minute=date_trunc('minute', tr.tx_block_time)
    and pu_eth.contract_address='{{eth_address}}'
    {% if is_incremental() %}
    and pu_eth.minute >= date_trunc("day", now() - interval '1 week')
    {% endif %}
left join {{ source('erc20_ethereum','evt_transfer') }} as erc20s
    on erc20s.evt_block_time=nft_mints.block_time
    and erc20s.from=nft_mints.to
    {% if is_incremental() %}
    and erc20s.evt_block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}
left join {{ source('prices','usd') }} as pu_erc20s
    on pu_erc20s.blockchain = 'optimism'
    and pu_erc20s.minute = date_trunc('minute', erc20s.evt_block_time)
    and erc20s.contract_address = pu_erc20s.contract_address
    {% if is_incremental() %}
    and pu_erc20s.minute >= date_trunc("day", now() - interval '1 week')
    {% endif %}
left join namespaces as ec
    on etxs.to=ec.address
left join {{ ref('nft_optimism_aggregators_legacy') }} as agg
    on etxs.to=agg.contract_address
left join nfts_per_tx as nft_count
    on nft_count.tx_hash=nft_mints.tx_hash
where
    nft_mints.from = '0x0000000000000000000000000000000000000000'
    {% if is_incremental() %}
    and nft_mints.block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}
    -- to exclude bridged L1 NFT collections to L2
    and bm.contract_address is null
    group by nft_mints.block_time, nft_mints.block_number, nft_mints.token_id, nft_mints.token_standard
    , nft_mints.amount, nft_mints.from, nft_mints.to, nft_mints.contract_address, etxs.to, nft_mints.evt_index
    , nft_mints.tx_hash, etxs.from, ec.namespace, tok.name, pu_erc20s.decimals, pu_eth.price, pu_erc20s.price
    , agg.name, agg.contract_address, nft_count.nfts_minted_in_tx, pu_erc20s.symbol, erc20s.contract_address, tr.tx_hash
