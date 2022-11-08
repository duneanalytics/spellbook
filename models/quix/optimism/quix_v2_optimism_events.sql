{{ config(
    schema = 'quix_v2_optimism',
    alias = 'events',
    partition_by = ['block_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'unique_trade_id']
    )
}}
with events_raw as (
    select 
      *
    from (
        select
            evt_block_number as block_number
            ,tokenId as token_id
            ,contract_address as project_contract_address
            ,evt_tx_hash as tx_hash
            ,evt_block_time as block_time
            ,buyer
            ,seller
            ,erc721address as nft_contract_address
            ,price as amount_raw
        from {{ source('quixotic_v2_optimism','ExchangeV2_evt_BuyOrderFilled') }}
        {% if is_incremental() %} -- this filter will only be applied on an incremental run
        where evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}

        union all 

        select
            evt_block_number as block_number
            ,tokenId as token_id
            ,contract_address as project_contract_address
            ,evt_tx_hash as tx_hash
            ,evt_block_time as block_time
            ,buyer
            ,seller
            ,erc721address as nft_contract_address
            ,price as amount_raw
        from {{ source('quixotic_v2_optimism','ExchangeV2_evt_DutchAuctionFilled') }}
        {% if is_incremental() %} -- this filter will only be applied on an incremental run
        where evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}

        union all 

        select
            evt_block_number as block_number
            ,tokenId as token_id
            ,contract_address as project_contract_address
            ,evt_tx_hash as tx_hash
            ,evt_block_time as block_time
            ,buyer
            ,seller
            ,erc721address as nft_contract_address
            ,price as amount_raw
        from {{ source('quixotic_v2_optimism','ExchangeV2_evt_SellOrderFilled') }}
        {% if is_incremental() %} -- this filter will only be applied on an incremental run
        where evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
    ) as x 
    where nft_contract_address != lower('0xbe81eabdbd437cba43e4c1c330c63022772c2520') -- --exploit contract
)
select
    'optimism' as blockchain
    ,'quix' as project
    ,'v2' as version
    ,TRY_CAST(date_trunc('DAY', er.block_time) AS date) AS block_date
    ,er.block_time
    ,er.token_id 
    ,n.name as collection
    ,er.amount_raw / power(10, t1.decimals) * p1.price as amount_usd
    ,'erc721' as token_standard
                -- ,trade_tyoe
                -- ,number_of_items
                -- ,trade_category
    ,'Trade' as evt_type
    ,er.seller
    ,case 
      when er.buyer = agg.contract_address then erct2.to
      else er.buyer 
    end as buyer
    ,er.amount_raw / power(10, t1.decimals) as amount_original
    ,er.amount_raw
    ,case 
      when (erc20.contract_address = '0x0000000000000000000000000000000000000000' or erc20.contract_address is null)
        then 'ETH'
        else t1.symbol
      end as currency_symbol
    ,case 
      when (erc20.contract_address = '0x0000000000000000000000000000000000000000' or erc20.contract_address is null) 
        then '0xdeaddeaddeaddeaddeaddeaddeaddeaddead0000'
        else erc20.contract_address
      end as currency_contract
    ,er.nft_contract_address
    ,er.project_contract_address
    ,agg.name as aggregator_name
    ,agg.contract_address as aggregator_address
    ,er.tx_hash
    ,er.block_number
    ,tx.from as tx_from
    ,tx.to as tx_to
                -- platform_fee_amount_raw,
                -- platform_fee_amount,
                -- platform_fee_amount_usd,
                -- platform_fee_percentage,
                -- royalty_fee_amount_raw,
                -- royalty_fee_amount,
                -- royalty_fee_amount_usd,
                -- royalty_fee_percentage,
                -- royalty_fee_receive_address,
                -- royalty_fee_currency_symbol,
            -- unique_trade_id

from events_raw as er 
join {{ source('optimism','transactions') }} as tx 
    on er.tx_hash = tx.hash
    and er.block_number = tx.block_number
    {% if not is_incremental() %}
    -- smallest block number for source tables above
    and tx.block_number > 2753613
    {% endif %}
    {% if is_incremental() %}
    and tx.block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}
left join {{ ref('nft_aggregators') }} as agg
    on agg.contract_address = tx.to 
    and agg.blockchain = 'optimism'
left join {{ ref('tokens_nft') }} n
    on n.contract_address = er.nft_contract_address 
    and n.blockchain = 'optimism'
left join {{ source('erc721_optimism','evt_transfer') }} as erct2 
    on erct2.evt_block_time=er.block_time
    and er.nft_contract_address=erct2.contract_address
    and erct2.evt_tx_hash=er.tx_hash
    and erct2.tokenId=er.token_id
    and erct2.from=er.buyer
    {% if not is_incremental() %}
    -- smallest block number for source tables above
    and erct2.evt_block_number > 2753613
    {% endif %}
    {% if is_incremental() %}
    and erct2.evt_block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}
left join {{ source('erc20_optimism','evt_transfer') }} as erc20 
    on erc20.evt_block_time=er.block_time
    and erc20.evt_tx_hash=er.tx_hash
    and erc20.to=er.seller
    {% if not is_incremental() %}
    -- smallest block number for source tables above
    and erc20.evt_block_number > 2753613
    {% endif %}
    {% if is_incremental() %}
    and erc20.evt_block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}
left join {{ ref('tokens_erc20') }} as t1
    on t1.contract_address =
        case when (erc20.contract_address = '0x0000000000000000000000000000000000000000' or erc20.contract_address is null)
        then '0xdeaddeaddeaddeaddeaddeaddeaddeaddead0000'
        else erc20.contract_address
        end 
    and t1.blockchain = 'optimism'
    left join {{ source('prices', 'usd') }} as p1
    on p1.contract_address =
        case when (erc20.contract_address = '0x0000000000000000000000000000000000000000' or erc20.contract_address is null)
        then '0xdeaddeaddeaddeaddeaddeaddeaddeaddead0000'
        else erc20.contract_address
        end
    and p1.minute = date_trunc('minute', er.block_time)
    and p1.blockchain = 'optimism'
    {% if is_incremental() %}
    and p1.minute >= date_trunc("day", now() - interval '1 week')
    {% endif %}

  