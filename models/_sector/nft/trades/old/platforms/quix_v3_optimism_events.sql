{{ config(
	tags=['legacy'],
	
    schema = 'quix_v3_optimism',
    alias = alias('events', legacy_model=True),
    partition_by = ['block_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'tx_hash', 'token_id', 'seller',  'evt_index']
    )
}}
{% set quix_fee_address_address = "0xec1557a67d4980c948cd473075293204f4d280fd" %}
{% set min_block_number = 3387715 %}
{% set project_start_date = '2022-02-10' %}     -- select time from optimism.blocks where `number` = 3387715


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
            ,contractAddress as nft_contract_address
            ,price as amount_raw
        from {{ source('quixotic_v3_optimism','ExchangeV3_evt_BuyOrderFilled') }}
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
            ,contractAddress as nft_contract_address
            ,price as amount_raw
        from {{ source('quixotic_v3_optimism','ExchangeV3_evt_DutchAuctionFilled') }}
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
            ,contractAddress as nft_contract_address
            ,price as amount_raw
        from {{ source('quixotic_v3_optimism','ExchangeV3_evt_SellOrderFilled') }}
        {% if is_incremental() %} -- this filter will only be applied on an incremental run
        where evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
    ) as x
    where nft_contract_address != lower('0xbe81eabdbd437cba43e4c1c330c63022772c2520') -- --exploit contract
)
,transfers as (
    -- eth royalities
    select
      tr.tx_block_number as block_number
      ,tr.tx_block_time as block_time
      ,tr.tx_hash
      ,tr.value
      ,tr.to
    from events_raw as er
    join {{ ref('transfers_optimism_eth_legacy') }} as tr
      on er.tx_hash = tr.tx_hash
      and er.block_number = tr.tx_block_number
      and tr.value_decimal > 0
      and tr.to not in (
        lower('{{quix_fee_address_address}}') --qx platform fee address
        ,er.seller
        ,er.project_contract_address
        ,lower('0x0000000000000000000000000000000000000000') -- v3 first few txs misconfigured to send fee to null address
      )
      {% if not is_incremental() %}
      -- smallest block number for source tables above
      and tr.tx_block_number >= '{{min_block_number}}'
      {% endif %}
      {% if is_incremental() %}
      and tr.tx_block_time >= date_trunc("day", now() - interval '1 week')
      {% endif %}

    union all

    -- erc20 royalities
    select
      erc20.evt_block_number as block_number
      ,erc20.evt_block_time as block_time
      ,erc20.evt_tx_hash as tx_hash
      ,erc20.value
      ,erc20.to
    from events_raw as er
    join {{ source('erc20_optimism','evt_transfer') }} as erc20
      on er.tx_hash = erc20.evt_tx_hash
      and er.block_number = erc20.evt_block_number
      and erc20.value is not null
      and erc20.to not in (
        lower('{{quix_fee_address_address}}') --qx platform fee address
        ,er.seller
        ,er.project_contract_address
        ,lower('0x0000000000000000000000000000000000000000') -- v3 first few txs misconfigured to send fee to null address
      )
      {% if not is_incremental() %}
      -- smallest block number for source tables above
      and erc20.evt_block_number >= '{{min_block_number}}'
      {% endif %}
      {% if is_incremental() %}
      and erc20.evt_block_time >= date_trunc("day", now() - interval '1 week')
      {% endif %}
)
,final as (
    select
        'optimism' as blockchain
        ,'quix' as project
        ,'v3' as version
        ,TRY_CAST(date_trunc('DAY', er.block_time) AS date) AS block_date
        ,er.block_time
        ,er.token_id
        ,n.name as collection
        ,er.amount_raw / power(10, t1.decimals) * p1.price as amount_usd
        ,case
        when erct2.evt_tx_hash is not null then 'erc721'
        when erc1155.evt_tx_hash is not null then 'erc1155'
        end as token_standard
        ,'Single Item Trade' as trade_type
        ,cast(1 as decimal(38, 0)) as number_of_items
        ,'Buy' as trade_category
        ,'Trade' as evt_type
        ,er.seller
        ,case
        when er.buyer = agg.contract_address then coalesce(erct2.to, erc1155.to)
        else er.buyer
        end as buyer
        ,er.amount_raw / power(10, t1.decimals) as amount_original
        ,cast(er.amount_raw as decimal(38, 0)) as amount_raw
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
        ,coalesce(erct2.evt_index,erc1155.evt_index, 1) as evt_index
        ,er.block_number
        ,tx.from as tx_from
        ,tx.to as tx_to
        ,ROUND((2.5*(er.amount_raw)/100),7) as platform_fee_amount_raw
        ,ROUND((2.5*((er.amount_raw / power(10,t1.decimals)))/100),7) AS platform_fee_amount
        ,ROUND((2.5*((er.amount_raw / power(10,t1.decimals)* p1.price))/100),7) AS platform_fee_amount_usd
        ,CAST(2.5 AS DOUBLE) AS platform_fee_percentage
        ,CAST(tr.value as double) as royalty_fee_amount_raw
        ,tr.value / power(10, t1.decimals) as royalty_fee_amount
        ,tr.value / power(10, t1.decimals) * p1.price as royalty_fee_amount_usd
        ,(tr.value / er.amount_raw * 100) as royalty_fee_percentage
        ,case when tr.value is not null then tr.to end as royalty_fee_receive_address
        ,case when tr.value is not null
            then case when (erc20.contract_address = '0x0000000000000000000000000000000000000000' or erc20.contract_address is null)
                then 'ETH' else t1.symbol end
            end as royalty_fee_currency_symbol
    from events_raw as er
    inner join {{ source('optimism','transactions') }} as tx
        on er.tx_hash = tx.hash
        and er.block_number = tx.block_number
        {% if not is_incremental() %}
        -- smallest block number for source tables above
        and tx.block_number >= '{{min_block_number}}'
        {% endif %}
        {% if is_incremental() %}
        and tx.block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
    left join {{ ref('nft_aggregators_legacy') }} as agg
        on agg.contract_address = tx.to
        and agg.blockchain = 'optimism'
    left join {{ ref('tokens_nft_legacy') }} n
        on n.contract_address = er.nft_contract_address
        and n.blockchain = 'optimism'
    left join {{ source('erc721_optimism','evt_transfer') }} as erct2
        on erct2.evt_block_time=er.block_time
        and er.nft_contract_address=erct2.contract_address
        and erct2.evt_tx_hash=er.tx_hash
        and erct2.tokenId=er.token_id
        and erct2.to=er.buyer
        {% if not is_incremental() %}
        -- smallest block number for source tables above
        and erct2.evt_block_number >= '{{min_block_number}}'
        {% endif %}
        {% if is_incremental() %}
        and erct2.evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
    left join {{ source('erc1155_optimism','evt_transfersingle') }} as erc1155
        on erc1155.evt_block_time=er.block_time
        and er.nft_contract_address=erc1155.contract_address
        and erc1155.evt_tx_hash=er.tx_hash
        and erc1155.id=er.token_id
        and erc1155.to=er.buyer
        {% if not is_incremental() %}
        -- smallest block number for source tables above
        and erc1155.evt_block_number >= '{{min_block_number}}'
        {% endif %}
        {% if is_incremental() %}
        and erc1155.evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
    left join {{ source('erc20_optimism','evt_transfer') }} as erc20
        on erc20.evt_block_time=er.block_time
        and erc20.evt_tx_hash=er.tx_hash
        and erc20.to=er.seller
        {% if not is_incremental() %}
        -- smallest block number for source tables above
        and erc20.evt_block_number >= '{{min_block_number}}'
        {% endif %}
        {% if is_incremental() %}
        and erc20.evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
    left join {{ ref('tokens_erc20_legacy') }} as t1
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
        {% if not is_incremental() %}
        and p1.minute >= '{{project_start_date}}'
        {% endif %}
    left join transfers as tr
        on tr.tx_hash = er.tx_hash
        and tr.block_number = er.block_number
)
select
    *
    ,concat(block_date, tx_hash, token_id, seller, evt_index) as unique_trade_id
from final
