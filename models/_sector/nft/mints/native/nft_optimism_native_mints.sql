{{ config(
        tags = ['dunesql'],
        schema = 'nft_optimism',
        alias = alias('native_mints'),
        partition_by = ['block_month'],
		file_format = 'delta',
		incremental_strategy = 'merge',
        unique_key = ['tx_hash','evt_index','token_id','number_of_items']
        )
}}
{% set eth_address = '0xdeaddeaddeaddeaddeaddeaddeaddeaddead0000' %}

with namespaces as (
    select
        contract_address as address
        ,coalesce(contract_project, contract_name, token_symbol) as namespace
	from {{ ref('contracts_optimism_contract_mapping') }}
)
, nfts_per_tx_tmp as (
    select
        tx_hash
        ,sum(amount) as nfts_minted_in_tx
        from {{ ref('nft_optimism_transfers') }}
        {% if is_incremental() %}
        where block_time >= date_trunc('day', now() - interval '7' Day)
        {% endif %}
    group by 1
)

, nfts_per_tx as (
    select
        tx_hash
        , case when nfts_minted_in_tx = UINT256 '0' THEN UINT256 '1' ELSE nfts_minted_in_tx END as nfts_minted_in_tx
    FROM
    nfts_per_tx_tmp
)

SELECT
blockchain
, project
, version
, block_time
, block_date
, block_month
, block_number
, token_id
, collection
, token_standard
, trade_type
, number_of_items
, trade_category
, evt_type
, seller
, buyer
, amount_raw
, amount_original
, amount_usd
, currency_symbol
, currency_contract
, nft_contract_address
, project_contract_address
, aggregator_name
, aggregator_address
, tx_hash
, tx_from
, tx_to
, platform_fee_amount_raw
, platform_fee_amount
, platform_fee_amount_usd
, platform_fee_percentage
, royalty_fee_receive_address
, royalty_fee_currency_symbol
, royalty_fee_amount_raw
, royalty_fee_amount
, royalty_fee_amount_usd
, royalty_fee_percentage
, evt_index
FROM
(
SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY tx_hash, evt_index, token_id, number_of_items ORDER BY amount_usd DESC NULLS LAST) as rank_index
FROM
(
select
    'optimism' as blockchain
    , coalesce(lower(ec.namespace), 'Unknown') as project
    , '' as version
    , nft_mints.block_time as block_time
    , CAST(date_trunc('day', nft_mints.block_time) as date) AS block_date
    , CAST(date_trunc('month', nft_mints.block_time) as date) AS block_month
    , nft_mints.block_number as block_number
    , nft_mints.token_id as token_id
    , tok.name as collection
    , nft_mints.token_standard
    , case
        when nft_mints.amount= UINT256 '1' then 'Single Item Mint'
        else 'Bundle Mint'
        end as trade_type
    , nft_mints.amount as number_of_items
    , 'Mint' as trade_category
    , 'Mint' as evt_type
    , nft_mints."from" as seller
    , nft_mints.to as buyer
    , case when tr.tx_hash is not null then 'ETH' else pu_erc20s.symbol end as currency_symbol
    , case when tr.tx_hash is not null then {{eth_address}} else erc20s.contract_address end as currency_contract
    , nft_mints.contract_address as nft_contract_address
    , etxs.to as project_contract_address
    , agg.name as aggregator_name
    , agg.contract_address as aggregator_address
    , nft_mints.tx_hash as tx_hash
    , etxs."from" as tx_from
    , etxs.to as tx_to
    , UINT256 '0' as platform_fee_amount_raw
    , double '0' as platform_fee_amount
    , double '0' as platform_fee_amount_usd
    , double '0' as platform_fee_percentage
    , CAST(NULL as VARBINARY) as royalty_fee_receive_address
    , '0' as royalty_fee_currency_symbol
    , UINT256 '0' as royalty_fee_amount_raw
    , double '0' as royalty_fee_amount
    , double '0' as royalty_fee_amount_usd
    , double '0' as royalty_fee_percentage
    , nft_mints.evt_index
    , cast(coalesce(sum(tr.value), sum(cast(erc20s.value as double)), 0)*(nft_mints.amount/nft_count.nfts_minted_in_tx) as UINT256) as amount_raw
    , coalesce(sum(tr.value_decimal), sum(cast(erc20s.value as double))/power(10, pu_erc20s.decimals))*(nft_mints.amount/nft_count.nfts_minted_in_tx) as amount_original
    , coalesce(pu_eth.price*sum(tr.value_decimal), pu_erc20s.price*sum(cast(erc20s.value as double))/power(10, pu_erc20s.decimals))*(nft_mints.amount/nft_count.nfts_minted_in_tx) as amount_usd
from {{ ref('nft_optimism_transfers') }} as nft_mints
left join {{ source('optimism','transactions') }} as etxs
    on etxs.block_time=nft_mints.block_time
    and etxs.hash=nft_mints.tx_hash
    {% if is_incremental() %}
    and etxs.block_time >= date_trunc('day', now() - interval '7' Day)
    {% endif %}
left join {{ ref('tokens_optimism_nft') }} as tok
    on tok.contract_address=nft_mints.contract_address
left join {{ ref('tokens_optimism_nft_bridged_mapping') }} as bm
    on bm.contract_address=nft_mints.contract_address
left join {{ ref('transfers_optimism_eth') }} as tr
    on nft_mints.tx_hash = tr.tx_hash
    and nft_mints.block_number = tr.tx_block_number
    and tr.value_decimal > 0
left join {{ source('prices','usd') }} as pu_eth
    on pu_eth.blockchain='optimism'
    and pu_eth.minute=date_trunc('minute', tr.tx_block_time)
    and pu_eth.contract_address= {{eth_address}}
    {% if is_incremental() %}
    and pu_eth.minute >= date_trunc('day', now() - interval '7' Day)
    {% endif %}
left join {{ source('erc20_ethereum','evt_transfer') }} as erc20s
    on erc20s.evt_block_time=nft_mints.block_time
    and erc20s."from"=nft_mints.to
    AND erc20s.evt_tx_hash = nft_mints.tx_hash
    AND (tr.value_decimal IS NULL OR CAST(tr.value_decimal as double) = 0)
    {% if is_incremental() %}
    and erc20s.evt_block_time >= date_trunc('day', now() - interval '7' Day)
    {% endif %}
left join {{ source('prices','usd') }} as pu_erc20s
    on pu_erc20s.blockchain = 'optimism'
    and pu_erc20s.minute = date_trunc('minute', erc20s.evt_block_time)
    and erc20s.contract_address = pu_erc20s.contract_address
    {% if is_incremental() %}
    and pu_erc20s.minute >= date_trunc('day', now() - interval '7' Day)
    {% endif %}
left join namespaces as ec
    on etxs.to=ec.address
left join {{ ref('nft_optimism_aggregators') }} as agg
    on etxs.to=agg.contract_address
left join nfts_per_tx as nft_count
    on nft_count.tx_hash=nft_mints.tx_hash
where
    nft_mints."from" = 0x0000000000000000000000000000000000000000
    {% if is_incremental() %}
    and nft_mints.block_time >= date_trunc('day', now() - interval '7' Day)
    {% endif %}
    -- to exclude bridged L1 NFT collections to L2
    and bm.contract_address is null
    group by nft_mints.block_time, nft_mints.block_number, nft_mints.token_id, nft_mints.token_standard
    , nft_mints.amount, nft_mints."from", nft_mints.to, nft_mints.contract_address, etxs.to, nft_mints.evt_index
    , nft_mints.tx_hash, etxs."from", ec.namespace, tok.name, pu_erc20s.decimals, pu_eth.price, pu_erc20s.price
    , agg.name, agg.contract_address, nft_count.nfts_minted_in_tx, pu_erc20s.symbol, erc20s.contract_address, tr.tx_hash
) tmp
) tmp_2
WHERE rank_index = 1
