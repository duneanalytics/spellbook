{{ config(
    schema = 'quix_v4_optimism',
    alias = alias('events'),
    tags = ['dunesql'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_number', 'tx_hash', 'token_id', 'seller',  'evt_index']
    )
}}
{% set quix_fee_address_address = '0xec1557a67d4980c948cd473075293204f4d280fd' %}
{% set min_block_number = 9162242 %}
{% set project_start_date = '2022-05-27' %}     -- select time from optimism.blocks where "number" = 9162242


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
        from {{ source('quixotic_v4_optimism','ExchangeV4_evt_BuyOrderFilled') }}
        {% if is_incremental() %} -- this filter will only be applied on an incremental run
        where evt_block_time >= date_trunc('day', now() - interval '7' day)
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
        from {{ source('quixotic_v4_optimism','ExchangeV4_evt_DutchAuctionFilled') }}
        {% if is_incremental() %} -- this filter will only be applied on an incremental run
        where evt_block_time >= date_trunc('day', now() - interval '7' day)
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
        from {{ source('quixotic_v4_optimism','ExchangeV4_evt_SellOrderFilled') }}
        {% if is_incremental() %}
        where evt_block_time >= date_trunc('day', now() - interval '7' day)
        {% endif %}
    ) as x
    where nft_contract_address != 0xbe81eabdbd437cba43e4c1c330c63022772c2520 -- --exploit contract
)
,transfers as (
    -- eth royalities
    select
      tr.tx_block_number as block_number
      ,tr.tx_block_time as block_time
      ,tr.tx_hash
      ,cast(tr.value as uint256) as value
      ,tr.to
    from events_raw as er
    join {{ ref('transfers_optimism_eth') }} as tr
      on er.tx_hash = tr.tx_hash
      and er.block_number = tr.tx_block_number
      and tr.value_decimal > 0
      and tr."from" in (er.project_contract_address, er.buyer) -- only include transfer from qx or buyer to royalty fee address
      and tr.to not in (
        {{quix_fee_address_address}} --qx platform fee address
        ,er.seller
        ,er.project_contract_address
        ,0x0000000000000000000000000000000000000000 -- v3 first few txs misconfigured to send fee to null address
        ,0x942f9ce5d9a33a82f88d233aeb3292e680230348 -- v4 there are txs via Ambire Wallet Contract Deployer to be excluded
      )
      {% if not is_incremental() %}
      -- smallest block number for source tables above
      and tr.tx_block_number >= {{min_block_number}}
      {% endif %}
      {% if is_incremental() %}
      and tr.tx_block_time >= date_trunc('day', now() - interval '7' day)
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
      and erc20."from" in (er.project_contract_address, er.buyer) -- only include transfer from qx to royalty fee address
      and erc20.to not in (
        {{quix_fee_address_address}} --qx platform fee address
        ,er.seller
        ,er.project_contract_address
        ,0x0000000000000000000000000000000000000000 -- v3 first few txs misconfigured to send fee to null address
        ,0x942f9ce5d9a33a82f88d233aeb3292e680230348 -- v4 there are txs via Ambire Wallet Contract Deployer to be excluded
      )
      {% if not is_incremental() %}
      -- smallest block number for source tables above
      and erc20.evt_block_number >= {{min_block_number}}
      {% endif %}
      {% if is_incremental() %}
      and erc20.evt_block_time >= date_trunc('day', now() - interval '7' day)
      {% endif %}
)
-- Not using this anymore as it provided incorrect prices
--,fill_missing_op_price as (
--    -- op price missing from prices.usd 2022-06-06
--    select
--      ,symbol
--      ,contract_address
--      ,avg(amount_usd/token_amount) as price
--    from (
--        select
--            block_time,
--            amount_usd,
--            token_bought_amount as token_amount,
--            token_bought_address as contract_address,
--            token_bought_symbol as symbol
--        from {{ ref('uniswap_optimism_trades') }}
--        where
--            token_bought_address = 0x4200000000000000000000000000000000000042
--            {% if is_incremental() %}
--            and block_time >= date_trunc('day', now() - interval '7' day)
--            {% endif %}
--
--        union all
--
--        select
--            block_time,
--            amount_usd,
--            token_sold_amount as token_amount,
--            token_sold_address as contract_address,
--            token_sold_symbol as symbol
--        from {{ ref('uniswap_optimism_trades') }}
--        where
--            token_bought_address = 0x4200000000000000000000000000000000000042
--            {% if is_incremental() %}
--            and block_time >= date_trunc('day', now() - interval '7' day)
--            {% endif %}
--    ) as x
--    group by 1, 2, 3
--)
,erc20_transfer as (
    select
      erc20.evt_block_time
      ,erc20.evt_block_number
      ,erc20.evt_tx_hash
      ,erc20.contract_address
      ,erc20.to
    from events_raw as er
    join {{ source('erc20_optimism','evt_transfer') }} as erc20
        on erc20.evt_block_time=er.block_time
            and erc20.evt_tx_hash=er.tx_hash
            and erc20.to=er.seller
            {% if not is_incremental() %}
            -- smallest block number for source tables above
            and erc20.evt_block_number >= {{min_block_number}}
            {% endif %}
            {% if is_incremental() %}
            and erc20.evt_block_time >= date_trunc('day', now() - interval '7' day)
            {% endif %}
    {{ dbt_utils.group_by(n=5) }}
)
,final as (
    select
        'optimism' as blockchain
        ,'quix' as project
        ,'v4' as version
        ,er.block_time
        ,er.token_id
        ,n.name as collection
        ,(er.amount_raw / power(10, t1.decimals)) * p1.price as amount_usd
        ,case
        when erct2.evt_tx_hash is not null then 'erc721'
        when erc1155.evt_tx_hash is not null then 'erc1155'
        end as token_standard
        ,'Single Item Trade' as trade_type
        ,uint256 '1' as number_of_items
        ,'Buy' as trade_category
        ,'Trade' as evt_type
        ,er.seller
        ,case
        when er.buyer = agg.contract_address then coalesce(erct2.to, erc1155.to)
        else er.buyer
        end as buyer
        ,er.amount_raw / power(10, t1.decimals) as amount_original
        ,er.amount_raw
        ,case
            when (erc20.contract_address = 0x0000000000000000000000000000000000000000 or erc20.contract_address is null)
                then 'ETH'
                else t1.symbol
            end as currency_symbol
        ,case
            when (erc20.contract_address = 0x0000000000000000000000000000000000000000 or erc20.contract_address is null)
                then 0xdeaddeaddeaddeaddeaddeaddeaddeaddead0000
                else erc20.contract_address
            end as currency_contract
        ,er.nft_contract_address
        ,er.project_contract_address
        ,agg.name as aggregator_name
        ,agg.contract_address as aggregator_address
        ,er.tx_hash
        ,coalesce(erct2.evt_index,erc1155.evt_index, 1) as evt_index
        ,er.block_number
        ,tx."from" as tx_from
        ,tx.to as tx_to
        ,cast((2.5*(er.amount_raw)/100)as uint256) as platform_fee_amount_raw
        ,2.5*((er.amount_raw / power(10,t1.decimals)))/100 AS platform_fee_amount
        ,2.5*(((er.amount_raw / power(10,t1.decimals))* p1.price))/100 AS platform_fee_amount_usd
        ,CAST(2.5 AS DOUBLE) AS platform_fee_percentage
        ,CAST(tr.value as uint256) as royalty_fee_amount_raw
        ,tr.value / power(10, t1.decimals) as royalty_fee_amount
        ,tr.value / power(10, t1.decimals) * p1.price as royalty_fee_amount_usd
        ,(tr.value / cast(er.amount_raw * 100 as double)) as royalty_fee_percentage
        ,case when tr.value is not null then tr.to end as royalty_fee_receive_address
        ,case when tr.value is not null
            then case when (erc20.contract_address = 0x0000000000000000000000000000000000000000 or erc20.contract_address is null)
                then 'ETH' else t1.symbol end
            end as royalty_fee_currency_symbol
    from events_raw as er
    inner join {{ source('optimism','transactions') }} as tx
        on er.tx_hash = tx.hash
        and er.block_number = tx.block_number
        {% if not is_incremental() %}
        -- smallest block number for source tables above
        and tx.block_number >= {{min_block_number}}
        {% endif %}
        {% if is_incremental() %}
        and tx.block_time >= date_trunc('day', now() - interval '7' day)
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
        and erct2.to=er.buyer
        {% if not is_incremental() %}
        -- smallest block number for source tables above
        and erct2.evt_block_number >= {{min_block_number}}
        {% endif %}
        {% if is_incremental() %}
        and erct2.evt_block_time >= date_trunc('day', now() - interval '7' day)
        {% endif %}
    left join {{ source('erc1155_optimism','evt_transfersingle') }} as erc1155
        on erc1155.evt_block_time=er.block_time
        and er.nft_contract_address=erc1155.contract_address
        and erc1155.evt_tx_hash=er.tx_hash
        and erc1155.id=er.token_id
        and erc1155.to=er.buyer
        {% if not is_incremental() %}
        -- smallest block number for source tables above
        and erc1155.evt_block_number >= {{min_block_number}}
        {% endif %}
        {% if is_incremental() %}
        and erc1155.evt_block_time >= date_trunc('day', now() - interval '7' day)
        {% endif %}
    left join erc20_transfer as erc20
        on erc20.evt_block_time=er.block_time
        and erc20.evt_tx_hash=er.tx_hash
        and erc20.to=er.seller
    left join {{ ref('tokens_erc20') }} as t1
        on t1.contract_address =
            case when (erc20.contract_address = 0x0000000000000000000000000000000000000000 or erc20.contract_address is null)
            then 0xdeaddeaddeaddeaddeaddeaddeaddeaddead0000
            else erc20.contract_address
            end
        and t1.blockchain = 'optimism'
    left join {{ source('prices', 'usd') }} as p1
        on p1.contract_address =
            case when (erc20.contract_address = 0x0000000000000000000000000000000000000000 or erc20.contract_address is null)
            then 0xdeaddeaddeaddeaddeaddeaddeaddeaddead0000
            else erc20.contract_address
            end
        and p1.minute = date_trunc('minute', er.block_time)
        and p1.blockchain = 'optimism'
        {% if is_incremental() %}
        and p1.minute >= date_trunc('day', now() - interval '7' day)
        {% endif %}
        {% if not is_incremental() %}
        and p1.minute >= TIMESTAMP '{{project_start_date}}'
        {% endif %}
    left join transfers as tr
        on tr.tx_hash = er.tx_hash
        and tr.block_number = er.block_number
)
select
    *
    ,concat(cast(tx_hash as varchar), cast(token_id as varchar),cast(evt_index as varchar)) as unique_trade_id
from final
