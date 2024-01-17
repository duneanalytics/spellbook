{{ config(
    schema = 'quix_v3_optimism',
    alias = 'base_trades',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_number','tx_hash','sub_tx_trade_id'],
    )
}}
{% set quix_fee_address_address = '0xec1557a67d4980c948cd473075293204f4d280fd' %}
{% set min_block_number = 3387715 %}
{% set project_start_date = '2022-02-10' %}     -- select time from optimism.blocks where "number" = 3387715


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
            ,evt_index
        from {{ source('quixotic_v3_optimism','ExchangeV3_evt_BuyOrderFilled') }}
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
            ,evt_index
        from {{ source('quixotic_v3_optimism','ExchangeV3_evt_DutchAuctionFilled') }}
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
            ,evt_index
        from {{ source('quixotic_v3_optimism','ExchangeV3_evt_SellOrderFilled') }}
        {% if is_incremental() %} -- this filter will only be applied on an incremental run
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
      and tr.to not in (
        {{quix_fee_address_address}} --qx platform fee address
        ,er.seller
        ,er.project_contract_address
        ,0x0000000000000000000000000000000000000000 -- v3 first few txs misconfigured to send fee to null address
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
      and erc20.to not in (
        {{quix_fee_address_address}} --qx platform fee address
        ,er.seller
        ,er.project_contract_address
        ,0x0000000000000000000000000000000000000000 -- v3 first few txs misconfigured to send fee to null address
      )
      {% if not is_incremental() %}
      -- smallest block number for source tables above
      and erc20.evt_block_number >= {{min_block_number}}
      {% endif %}
      {% if is_incremental() %}
      and erc20.evt_block_time >= date_trunc('day', now() - interval '7' day)
      {% endif %}
)
,base_trades as (
    select
        'optimism' as blockchain
        ,'quix' as project
        ,'v3' as project_version
        ,er.block_time
        ,cast(date_trunc('day', er.block_time) as date) as block_date
        ,cast(date_trunc('month', er.block_time) as date) as block_month
        ,er.token_id as nft_token_id
        ,'secondary' as trade_type
        ,uint256 '1'as nft_amount
        ,'Buy' as trade_category
        ,er.seller
        ,er.buyer
        ,er.amount_raw as price_raw
        ,case
            when (erc20.contract_address = 0x0000000000000000000000000000000000000000 or erc20.contract_address is null)
                then 0xdeaddeaddeaddeaddeaddeaddeaddeaddead0000
                else erc20.contract_address
            end as currency_contract
        ,er.nft_contract_address
        ,er.project_contract_address
        ,er.tx_hash
        ,er.block_number
        ,cast((double '2.5'*(er.amount_raw)/100) as uint256) as platform_fee_amount_raw
        ,CAST(tr.value as uint256) as royalty_fee_amount_raw
        ,case when tr.value is not null then tr.to end as royalty_fee_address
        ,cast(null as varbinary) as platform_fee_address
        ,er.evt_index as sub_tx_trade_id
    from events_raw as er
    left join {{ source('erc20_optimism','evt_transfer') }} as erc20
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
    left join transfers as tr
        on tr.tx_hash = er.tx_hash
        and tr.block_number = er.block_number
)

-- this will be removed once tx_from and tx_to are available in the base event tables
{{ add_nft_tx_data('base_trades', 'optimism') }}
