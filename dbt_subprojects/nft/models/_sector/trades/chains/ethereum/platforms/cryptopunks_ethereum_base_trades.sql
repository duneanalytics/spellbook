{{ config(
    schema = 'cryptopunks_ethereum',
    
    alias = 'base_trades',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_number','tx_hash','sub_tx_trade_id'],
    )
}}


with accepted_bid_prices as (
    select
    call_block_number
    ,call_tx_hash
    ,call.punkIndex
    ,max_by(bid.value, evt_block_number) as latest_bid
    ,max_by(bid.fromAddress, evt_block_number) as latest_bidder
    from {{ source('cryptopunks_ethereum','CryptoPunksMarket_call_acceptBidForPunk')}} call
    left join {{ source('cryptopunks_ethereum','CryptoPunksMarket_evt_PunkBidEntered') }} bid
        on call_success
        and call.call_block_number >= bid.evt_block_number
        and call.punkIndex = bid.punkIndex
    WHERE call_success
    {% if is_incremental() %}
    AND call.{{incremental_predicate('call_block_time')}}
    {% endif %}
    group by 1,2,3
)

select
         'ethereum' as blockchain
        , 'cryptopunks' as project
        , 'v1' as project_version
        , evt.evt_block_time as block_time
        , evt.evt_block_number as block_number
        , evt.evt_tx_hash as tx_hash
        , evt.contract_address as project_contract_address
        , evt.evt_index as sub_tx_trade_id
        , case when call.latest_bid is null
            then 'Buy'
            else 'Bid accepted' end as trade_category
        , 'secondary' as trade_type
        , coalesce(call.latest_bidder,evt.toAddress) as buyer
        , evt.fromAddress as seller
        , evt.punkIndex as nft_token_id
        , uint256 '1' as nft_amount
        , evt.contract_address as nft_contract_address
        , cast(coalesce(call.latest_bid, evt.value) as UINT256) as price_raw
        , {{ var("ETH_ERC20_ADDRESS") }} AS currency_contract -- all trades are in ETH
        , UINT256 '0' as platform_fee_amount_raw
        , UINT256 '0' as royalty_fee_amount_raw
        , cast(null as varbinary) as platform_fee_address
        , cast(null as varbinary) as royalty_fee_address
from {{ source('cryptopunks_ethereum','CryptoPunksMarket_evt_PunkBought') }} evt
left join accepted_bid_prices call
on evt.evt_block_number = call.call_block_number
    and evt.evt_tx_hash = call.call_tx_hash
    and evt.punkIndex = call.punkIndex
where evt.evt_tx_hash not in (0x92488a00dfa0746c300c66a716e6cc11ba9c0f9d40d8c58e792cc7fcebf432d0 -- flash loan https://twitter.com/cryptopunksnfts/status/1453903818308083720
                     )
{% if is_incremental() %}
and evt.{{incremental_predicate('evt_block_time')}}
{% endif %}
