-- ## this function consists of 4 parts,
-- ## p1_ : seaport."Seaport_call_fulfillBasicOrder"
-- ## p2_ : seaport."Seaport_call_fulfillAvailableAdvancedOrders"
-- ##       seaport."Seaport_call_fulfillAvailableOrders"
-- ## p3_ : seaport."Seaport_call_fulfillOrder"
-- ##       seaport."Seaport_call_fulfillAdvancedOrder"
-- ## p4_ : seaport."Seaport_call_matchOrders"
-- ##     : seaport."Seaport_call_matchAdvancedOrders"

{{ config(
    alias = 'transfers',
    partition_by = ['block_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'unique_trade_id']
    )
}}

with p1_call as (
    select 'basic_order' as main_type
          ,call_tx_hash as tx_hash
          ,call_block_time as block_time
          ,call_block_number as block_number
          ,max(get_json_object(parameters, "$.basicOrderType")) as order_type_id
      from {{ source('seaport_ethereum','Seaport_call_fulfillBasicOrder') }}
      {% if is_incremental() %} -- this filter will only be applied on an incremental run
      where call_block_time >= (select max(block_time) from {{ this }})
      {% endif %}
     group by 1,2,3,4
)

,p1_evt as (
    select c.main_type
          ,c.tx_hash
          ,c.block_time
          ,c.block_number
          ,c.order_type_id
          ,'offer' as sub_type
          ,offer_idx as sub_idx
          ,e.offerer as sender
          ,e.recipient as receiver
          ,e.zone
          ,concat('0x',substr(get_json_object(offer2, "$.token"),3,40)) as token_contract_address
          ,get_json_object(offer2, "$.amount") as original_amount
          ,get_json_object(offer2, "$.itemType") as item_type
          ,get_json_object(offer2, "$.identifier") as token_id
          ,e.contract_address as exchange_contract_address
          ,e.evt_index
      from
          (select *, posexplode(offer) as (offer_idx, offer2) from {{ source('seaport_ethereum','Seaport_evt_OrderFulfilled') }}
               {% if is_incremental() %} -- this filter will only be applied on an incremental run
               where evt_block_time >= (select max(block_time) from {{ this }})
               {% endif %}
            ) e
          inner join p1_call c on c.tx_hash = e.evt_tx_hash
                      union all
    select c.main_type
          ,c.tx_hash
          ,c.block_time
          ,c.block_number
          ,c.order_type_id
          ,'consideration' as sub_type
          ,consideration_idx as sub_idx
          ,e.recipient as sender
          ,concat('0x',substr(get_json_object(consideration2, "$.recipient"),3,40)) as receiver
          ,e.zone
          ,concat('0x',substr(get_json_object(consideration2, "$.token"),3,40)) as token_contract_address
          ,get_json_object(consideration2, "$.amount") as original_amount
          ,get_json_object(consideration2, "$.itemType") as item_type
          ,get_json_object(consideration2, "$.identifier") as token_id
          ,e.contract_address as exchange_contract_address
          ,e.evt_index
      from
        (select *, posexplode(consideration) as (consideration_idx, consideration2) from {{ source('seaport_ethereum','Seaport_evt_OrderFulfilled') }}
            {% if is_incremental() %} -- this filter will only be applied on an incremental run
            where evt_block_time >= (select max(block_time) from {{ this }})
            {% endif %}
            ) e
        inner join p1_call c on c.tx_hash = e.evt_tx_hash
     )


,p1_add_rn as (select (max(case when purchase_method = 'Offer Accepted' and sub_type = 'offer' and sub_idx = 0 then token_contract_address
                     when purchase_method = 'Buy Now' and sub_type = 'consideration' then token_contract_address
                end) over (partition by tx_hash, evt_index)) as avg_original_currency_contract
          ,sum(case when purchase_method = 'Offer Accepted' and sub_type = 'offer' and sub_idx = 0 then original_amount
                    when purchase_method = 'Buy Now' and sub_type = 'consideration' then original_amount
               end) over (partition by tx_hash, evt_index)
           / nft_transfer_count as avg_original_amount
          ,sum(case when fee_royalty_yn = 'fee' then original_amount end) over (partition by tx_hash, evt_index) / nft_transfer_count as avg_fee_amount
          ,sum(case when fee_royalty_yn = 'royalty' then original_amount end) over (partition by tx_hash, evt_index) / nft_transfer_count as avg_royalty_amount
          ,(max(case when fee_royalty_yn = 'fee' then receiver end) over (partition by tx_hash, evt_index)) as avg_fee_receive_address
          ,(max(case when fee_royalty_yn = 'royalty' then receiver end) over (partition by tx_hash, evt_index)) as avg_royalty_receive_address
          ,a.*
      from (select case when purchase_method = 'Offer Accepted' and sub_type = 'consideration' and fee_royalty_idx = 1 then 'fee'
                        when purchase_method = 'Offer Accepted' and sub_type = 'consideration' and fee_royalty_idx = 2 then 'royalty'
                        when purchase_method = 'Buy Now' and sub_type = 'consideration' and fee_royalty_idx = 2 then 'fee'
                        when purchase_method = 'Buy Now' and sub_type = 'consideration' and fee_royalty_idx = 3 then 'royalty'
                   end as fee_royalty_yn
                  ,case when purchase_method = 'Offer Accepted' and main_type = 'order' then 'Individual Offer'
                        when purchase_method = 'Offer Accepted' and main_type = 'basic_order' then 'Individual Offer'
                        when purchase_method = 'Offer Accepted' and main_type = 'advanced_order' then 'Collection/Trait Offers'
                        else 'Buy Now'
                   end as order_type
                  ,a.*
              from (select (count(case when item_type in ('2','3') then 1 end) over (partition by tx_hash, evt_index)) as nft_transfer_count
                          ,(sum(case when item_type in ('0','1') then 1 end) over (partition by tx_hash, evt_index, sub_type order by sub_idx)) as fee_royalty_idx
                          ,case when max(case when (sub_type,sub_idx,item_type) in (('offer',0,'1')) then 1 else 0 end) over (partition by tx_hash) = 1 then 'Offer Accepted'
                                else 'Buy Now'
                           end as purchase_method
                          ,a.*
                      from p1_evt a
                    ) a
            ) a
)

,p1_txn_level as (
    select main_type
          ,sub_idx
          ,tx_hash
          ,block_time
          ,block_number
          ,zone
          ,exchange_contract_address
          ,evt_index
          ,order_type
          ,purchase_method
          ,receiver as buyer
          ,sender as seller
          ,avg_original_amount as original_amount
          ,avg_original_currency_contract as original_currency_contract
          ,avg_fee_receive_address as fee_receive_address
          ,avg_fee_amount as fee_amount
          ,avg_original_currency_contract as fee_currency_contract
          ,avg_royalty_receive_address as royalty_receive_address
          ,avg_royalty_amount as royalty_amount
          ,avg_original_currency_contract as royalty_currency_contract
          ,token_contract_address as nft_contract_address
          ,token_id as nft_token_id
          ,nft_transfer_count
          ,original_amount as nft_item_count
          ,coalesce(avg_original_amount,0) + coalesce(avg_fee_amount,0) + coalesce(avg_royalty_amount,0) as attempt_amount
          ,0 as revert_amount
          ,false as reverted
          ,case when nft_transfer_count > 1 then true else false end as price_estimated
          ,'' as offer_order_type
          ,item_type
          ,order_type_id
      from p1_add_rn a
     where item_type in ('2','3')
)




,p1_seaport_transfers as (select
          'ethereum' as blockchain
          ,'seaport' as project
          ,'v1' as version
          ,TRY_CAST(date_trunc('DAY', a.block_time) AS date) AS block_date
          ,a.block_time
          ,a.block_number
          ,a.nft_token_id as token_id
          ,n.name as collection
          ,a.attempt_amount / power(10,t1.decimals) * p1.price as amount_usd
          ,case when item_type = '2' then 'erc721'
                when item_type = '3' then 'erc1155'
           end as token_standard
          ,case when order_type = 'Bulk Purchase' then 'Bulk Purchase'
                when nft_transfer_count = 1 then 'Single Item Trade'
                else 'Bundle Trade'
           end as trade_type
          ,nft_item_count as number_of_items
          ,a.purchase_method as trade_category
          ,'Trade' as evt_type
          ,concat('0x',substr(seller,3,40)) as seller
          ,concat('0x',substr(buyer,3,40)) as buyer
          ,a.original_amount / power(10,t1.decimals) as amount_original
          ,a.original_amount as amount_raw
          ,case when a.original_currency_contract = '0x0000000000000000000000000000000000000000' then 'ETH'
                else t1.symbol
           end as currency_symbol
          ,case when a.original_currency_contract = '0x0000000000000000000000000000000000000000' then
          '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
                else a.original_currency_contract
           end as currency_contract
          ,nft_contract_address
          ,a.exchange_contract_address as project_contract_address
          ,agg.name as aggregator_name
          ,agg.contract_address as aggregator_address
          ,a.tx_hash
          ,tx.from as tx_from
          ,tx.to as tx_to
          ,ROUND((2.5*(a.original_amount)/100),7) AS platform_fee_amount_raw
          ,ROUND((2.5*((a.original_amount / power(10,t1.decimals)))/100),7) AS platform_fee_amount
          ,ROUND((2.5*((a.original_amount / power(10,t1.decimals)* p1.price))/100),7) AS platform_fee_amount_usd
          ,'2.5' as platform_fee_percentage
          ,a.royalty_amount as royalty_fee_amount_raw
          ,a.royalty_amount / power(10,t1.decimals) as royalty_fee_amount
          ,a.royalty_amount / power(10,t1.decimals) * p1.price as royalty_fee_amount_usd
          ,(a.royalty_amount / a.original_amount * 100)::string  AS royalty_fee_percentage
          ,a.royalty_receive_address as royalty_fee_receive_address
          ,case when royalty_amount > 0 and a.original_currency_contract =
          '0x0000000000000000000000000000000000000000' then 'ETH'
                when royalty_amount > 0 then t1.symbol
          end as royalty_fee_currency_symbol
          ,a.tx_hash || '-' || a.nft_token_id || '-' || a.original_amount::string || '-' ||  concat('0x',substr(seller,3,40)) || '-' ||
          order_type_id::string || '-' || cast(row_number () over (partition by tx_hash order by sub_idx) as
          string) as unique_trade_id,
          a.zone
      from p1_txn_level a
        left join {{ source('ethereum','transactions') }} tx
            on tx.hash = a.tx_hash
            {% if not is_incremental() %}
            and tx.block_number > 14801608
            {% endif %}
            {% if is_incremental() %}
            and tx.block_time >= date_trunc("day", now() - interval '1 week')
            {% endif %}
        left join {{ ref('nft_ethereum_aggregators') }} agg
            ON agg.contract_address = tx.to
        left join {{ ref('tokens_ethereum_nft') }} n
            on n.contract_address = nft_contract_address
        left join {{ ref('tokens_ethereum_erc20') }} t1
            on t1.contract_address =
                case when a.original_currency_contract = '0x0000000000000000000000000000000000000000'
                then '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
                else a.original_currency_contract
                end
          left join {{ source('prices', 'usd') }} p1
            on p1.contract_address =
                case when a.original_currency_contract = '0x0000000000000000000000000000000000000000'
                then '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
                else a.original_currency_contract
                end
            and p1.minute = date_trunc('minute', a.block_time)
            and p1.blockchain = 'ethereum'
            {% if is_incremental() %}
            and p1.minute >= date_trunc("day", now() - interval '1 week')
            {% endif %}
            )

,p2_call as (
    select 'available_advanced_orders' as main_type
          ,'bulk' as sub_type
          ,idx as sub_idx
          ,get_json_object(get_json_object(each, "$.parameters"), "$.zone") as zone
          ,get_json_object(get_json_object(each, "$.parameters"), "$.offerer") as offerer
          ,get_json_object(get_json_object(get_json_object(each, "$.parameters"), "$.offer[0]"), "$.token") as offer_token
          ,get_json_object(get_json_object(get_json_object(each, "$.parameters"), "$.offer[0]"), "$.itemType") as offer_item_type
          ,get_json_object(get_json_object(get_json_object(each, "$.parameters"), "$.offer[0]"), "$.identifierOrCriteria") as offer_identifier
          ,get_json_object(get_json_object(each, "$.parameters"), "$.orderType") as offer_order_type
          ,get_json_object(get_json_object(get_json_object(each, "$.parameters"), "$.consideration[0]"), "$.token") as price_token
          ,get_json_object(get_json_object(get_json_object(each, "$.parameters"), "$.consideration[0]"), "$.itemType") as price_item_type
          ,get_json_object(get_json_object(get_json_object(each, "$.parameters"), "$.consideration[0]"), "$.startAmount") as price_amount
          ,get_json_object(get_json_object(get_json_object(each, "$.parameters"), "$.consideration[1]"), "$.startAmount") as fee_amount
          ,get_json_object(get_json_object(get_json_object(each, "$.parameters"), "$.consideration[2]"), "$.startAmount") as royalty_amount
          ,c.call_tx_hash as tx_hash
          ,c.call_block_time as block_time
          ,c.call_block_number as block_number
          ,c.contract_address as exchange_contract_address
      from (select *, posexplode(advancedOrders) as (idx, each) from {{ source('seaport_ethereum','Seaport_call_fulfillAvailableAdvancedOrders') }}
      {% if is_incremental() %} -- this filter will only be applied on an incremental run
      where call_block_time >= (select max(block_time) from {{ this }})
      {% endif %}
      ) c
       where call_success

                                                  union all
      select 'available_orders' as main_type
          ,'bulk' as sub_type
          ,idx as sub_idx
          ,get_json_object(get_json_object(each, "$.parameters"), "$.zone") as zone
          ,get_json_object(get_json_object(each, "$.parameters"), "$.offerer") as offerer
          ,get_json_object(get_json_object(get_json_object(each, "$.parameters"), "$.offer[0]"), "$.token") as offer_token
          ,get_json_object(get_json_object(get_json_object(each, "$.parameters"), "$.offer[0]"), "$.itemType") as offer_item_type
          ,get_json_object(get_json_object(get_json_object(each, "$.parameters"), "$.offer[0]"), "$.identifierOrCriteria") as offer_identifier
          ,get_json_object(get_json_object(each, "$.parameters"), "$.orderType") as offer_order_type
          ,get_json_object(get_json_object(get_json_object(each, "$.parameters"), "$.consideration[0]"), "$.token") as price_token
          ,get_json_object(get_json_object(get_json_object(each, "$.parameters"), "$.consideration[0]"), "$.itemType") as price_item_type
          ,get_json_object(get_json_object(get_json_object(each, "$.parameters"), "$.consideration[0]"), "$.startAmount") as price_amount
          ,get_json_object(get_json_object(get_json_object(each, "$.parameters"), "$.consideration[1]"), "$.startAmount") as fee_amount
          ,get_json_object(get_json_object(get_json_object(each, "$.parameters"), "$.consideration[2]"), "$.startAmount") as royalty_amount
          ,c.call_tx_hash as tx_hash
          ,c.call_block_time as block_time
          ,c.call_block_number as block_number
          ,c.contract_address as exchange_contract_address
      from (select *, posexplode(orders) as (idx, each) from {{ source('seaport_ethereum','Seaport_call_fulfillAvailableOrders') }}
      {% if is_incremental() %} -- this filter will only be applied on an incremental run
      where call_block_time >= (select max(block_time) from {{ this }})
      {% endif %}
      ) c
      where call_success
)
,p2_evt as (
 select c.*
          ,evt_tx_hash
          ,e.recipient
          ,get_json_object(offer[0], "$.amount") as evt_token_amount
          ,get_json_object(consideration[0], "$.token") as evt_price_token
          ,get_json_object(consideration[0], "$.amount") as evt_price_amount
          ,get_json_object(consideration[0], "$.itemType") as evt_price_item_type
          ,get_json_object(consideration[0], "$.recipient") as evt_price_recipient
          ,get_json_object(consideration[0], "$.identifier") as evt_price_identifier
          ,get_json_object(consideration[0], "$.token") as evt_fee_token
          ,get_json_object(consideration[1], "$.amount") as evt_fee_amount
          ,get_json_object(consideration[1], "$.itemType") as evt_fee_item_type
          ,get_json_object(consideration[1], "$.recipient") as evt_fee_recipient
          ,get_json_object(consideration[1], "$.identifier") as evt_fee_identifier
          ,get_json_object(consideration[2], "$.token") as evt_royalty_token
          ,get_json_object(consideration[2], "$.amount") as evt_royalty_amount
          ,get_json_object(consideration[2], "$.itemType") as evt_royalty_item_type
          ,get_json_object(consideration[2], "$.recipient") as evt_royalty_recipient
          ,get_json_object(consideration[2], "$.identifier") as evt_royalty_identifier
          ,e.evt_index
      from p2_call c
            inner join {{ source('seaport_ethereum','Seaport_evt_OrderFulfilled') }} e
            on e.evt_tx_hash = c.tx_hash
            and e.offerer = concat('0x',substr(c.offerer,3,40))
            and get_json_object(e.offer[0], "$.token") = c.offer_token
            and get_json_object(e.offer[0], "$.identifier") = c.offer_identifier
            and get_json_object(e.offer[0], "$.itemType") = c.offer_item_type
)
,p2_transfer_level as (
    select a.main_type
          ,a.sub_idx
          ,a.tx_hash
          ,a.block_time
          ,a.block_number
          ,a.zone
          ,a.exchange_contract_address
          ,offer_token as nft_address
          ,offer_identifier as nft_token_id
          ,recipient as buyer
          ,offerer as seller
          ,offer_item_type as offer_item_type
          ,offer_order_type as offer_order_type
          ,offer_identifier as nft_token_id_dcnt
          ,price_token as price_token
          ,price_item_type as price_item_type
          ,price_amount as price_amount
          ,fee_amount as fee_amount
          ,royalty_amount as royalty_amount
          ,evt_token_amount as evt_token_amount
          ,evt_price_amount as evt_price_amount
          ,evt_fee_amount as evt_fee_amount
          ,evt_royalty_amount as evt_royalty_amount
          ,evt_fee_token as evt_fee_token
          ,evt_royalty_token as evt_royalty_token
          ,evt_fee_recipient as evt_fee_recipient
          ,evt_royalty_recipient as evt_royalty_recipient
          ,coalesce(price_amount,0) + coalesce(fee_amount,0) + coalesce(royalty_amount,0) as attempt_amount
          ,case when evt_tx_hash is not null then coalesce(price_amount,0) + coalesce(fee_amount,0) + coalesce(royalty_amount,0) end as trade_amount
          ,case when evt_tx_hash is null then coalesce(price_amount,0) + coalesce(fee_amount,0) + coalesce(royalty_amount,0) else 0 end as revert_amount
          ,case when evt_tx_hash is null then true else false end as reverted
          ,'Bulk Purchase' as trade_type
          ,'Bulk Purchase' as order_type
          ,'Buy Now' as purchase_method
      from p2_evt a
)

,p2_seaport_transfers as (select
          'ethereum' as blockchain
          ,'seaport' as project
          ,'v1' as version
          ,TRY_CAST(date_trunc('DAY', a.block_time) AS date) AS block_date
          ,a.block_time
          ,a.block_number
          ,a.nft_token_id as token_id
          ,n.name as collection
          ,a.attempt_amount / power(10,t1.decimals) * p1.price as amount_usd
          ,case when offer_item_type = '2' then 'erc721'
                when offer_item_type = '3' then 'erc1155'
           end as token_standard
          ,trade_type
          ,evt_token_amount as number_of_items
          ,a.purchase_method as trade_category
          ,'Trade' as evt_type
          ,concat('0x',substr(seller,3,40)) as seller
          ,concat('0x',substr(buyer,3,40)) as buyer
          ,a.attempt_amount / power(10,t1.decimals) as amount_original
          ,a.attempt_amount as amount_raw
          ,case when concat('0x',substr(a.price_token,3,40)) =
          '0x0000000000000000000000000000000000000000' then 'ETH'
                else t1.symbol
           end as currency_symbol
          ,case when concat('0x',substr(a.price_token,3,40)) =
          '0x0000000000000000000000000000000000000000' then '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
                else concat('0x',substr(a.price_token,3,40))
           end as currency_contract
          ,concat('0x',substr(a.nft_address,3,40)) as nft_contract_address
          ,a.exchange_contract_address as project_contract_address
          ,agg.name as aggregator_name
          ,agg.contract_address as aggregator_address
          ,a.tx_hash
          ,tx.from as tx_from
          ,tx.to as tx_to
          ,ROUND((2.5*(a.attempt_amount)/100),7) AS platform_fee_amount_raw
          ,ROUND((2.5*((a.attempt_amount / power(10,t1.decimals)))/100),7) AS platform_fee_amount
          ,ROUND((2.5*((a.attempt_amount / power(10,t1.decimals)* p1.price))/100),7) AS platform_fee_amount_usd
          ,'2.5' as platform_fee_percentage
          ,a.evt_royalty_amount as royalty_fee_amount_raw
          ,a.evt_royalty_amount / power(10,t1.decimals) as royalty_fee_amount
          ,a.evt_royalty_amount / power(10,t1.decimals) * p1.price as royalty_fee_amount_usd
          ,(a.evt_royalty_amount / a.attempt_amount * 100)::string  AS royalty_fee_percentage
          ,case when evt_royalty_amount > 0 then concat('0x',substr(evt_royalty_recipient,3,40)) end as
          royalty_fee_receive_address
          ,case when evt_royalty_amount > 0 and concat('0x',substr(a.evt_royalty_token,3,40)) =
          '0x0000000000000000000000000000000000000000' then 'ETH'
                when evt_royalty_amount > 0 then t1.symbol
          end as royalty_fee_currency_symbol
          ,a.tx_hash || '-' || a.nft_token_id || '-' || a.attempt_amount::string || '-' ||  concat('0x',substr(seller,3,40)) || '-' ||
          cast(row_number () over (partition by tx_hash order by sub_idx) as
          string) as unique_trade_id,
          a.zone
      from p2_transfer_level a
        left join {{ source('ethereum','transactions') }} tx
            on tx.hash = a.tx_hash
            {% if not is_incremental() %}
            and tx.block_number > 14801608
            {% endif %}
            {% if is_incremental() %}
            and tx.block_time >= date_trunc("day", now() - interval '1 week')
            {% endif %}
        left join {{ ref('nft_ethereum_aggregators') }} agg
            ON agg.contract_address = tx.to
        left join {{ ref('tokens_ethereum_nft') }} n
            on n.contract_address = concat('0x',substr(a.nft_address,3,40))
        left join {{ ref('tokens_ethereum_erc20') }} t1
            on t1.contract_address =
                case when concat('0x',substr(a.price_token,3,40)) = '0x0000000000000000000000000000000000000000'
                then '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
                else concat('0x',substr(a.price_token,3,40))
                end
          left join {{ source('prices', 'usd') }} p1
            on p1.contract_address =
                case when concat('0x',substr(a.price_token,3,40)) = '0x0000000000000000000000000000000000000000'
                then '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
                else concat('0x',substr(a.price_token,3,40))
                end
            and p1.minute = date_trunc('minute', a.block_time)
            and p1.blockchain = 'ethereum'
            {% if is_incremental() %}
            and p1.minute >= date_trunc("day", now() - interval '1 week')
            {% endif %}
            )

,p3_call as (select 'order' as main_type
          ,call_tx_hash as tx_hash
          ,call_block_time as block_time
          ,call_block_number as block_number
          ,max(get_json_object(get_json_object(order, "$.parameters"), "$.orderType")) as order_type_id
      from {{ source('seaport_ethereum','Seaport_call_fulfillOrder') }}
      {% if is_incremental() %} -- this filter will only be applied on an incremental run
      where call_block_time >= (select max(block_time) from {{ this }})
      {% endif %}
     group by 1,2,3,4
     union all
    select 'advanced_order' as main_type
          ,call_tx_hash as tx_hash
          ,call_block_time as block_time
          ,call_block_number as block_number
          ,max(get_json_object(get_json_object(advancedOrder, "$.parameters"), "$.orderType")) as order_type_id
      from {{ source('seaport_ethereum','Seaport_call_fulfillAdvancedOrder') }}
      {% if is_incremental() %} -- this filter will only be applied on an incremental run
      where call_block_time >= (select max(block_time) from {{ this }})
      {% endif %}
      group by 1,2,3,4)

,p3_evt as (select c.main_type
            ,c.tx_hash
            ,c.block_time
            ,c.block_number
            ,c.order_type_id
            ,'offer' as sub_type
            ,offer_idx as sub_idx
            ,e.offerer as sender
            ,e.recipient as receiver
            ,e.zone
            ,concat('0x',substr(get_json_object(offer2, "$.token"),3,40)) as token_contract_address
            ,get_json_object(offer2, "$.amount") as original_amount
            ,get_json_object(offer2, "$.itemType") as item_type
            ,get_json_object(offer2, "$.identifier") as token_id
            ,e.contract_address as exchange_contract_address
            ,e.evt_index
        from
        (select *, posexplode(offer) as (offer_idx, offer2) from {{ source('seaport_ethereum','Seaport_evt_OrderFulfilled') }}
        {% if is_incremental() %} -- this filter will only be applied on an incremental run
        where evt_block_time >= (select max(block_time) from {{ this }})
        {% endif %}
        ) e
        inner join p3_call c on c.tx_hash = e.evt_tx_hash
        union all
        select c.main_type
            ,c.tx_hash
            ,c.block_time
            ,c.block_number
            ,c.order_type_id
            ,'consideration' as sub_type
            ,consideration_idx as sub_idx
            ,e.recipient as sender
            ,concat('0x',substr(get_json_object(consideration2, "$.recipient"),3,40)) as receiver
            ,e.zone
            ,concat('0x',substr(get_json_object(consideration2, "$.token"),3,40)) as token_contract_address
            ,get_json_object(consideration2, "$.amount") as original_amount
            ,get_json_object(consideration2, "$.itemType") as item_type
            ,get_json_object(consideration2, "$.identifier") as token_id
            ,e.contract_address as exchange_contract_address
            ,e.evt_index
        from
        (select *, posexplode(consideration) as (consideration_idx, consideration2) from {{ source('seaport_ethereum','Seaport_evt_OrderFulfilled') }}
          {% if is_incremental() %} -- this filter will only be applied on an incremental run
          where evt_block_time >= (select max(block_time) from {{ this }})
          {% endif %}
          ) e
        inner join p3_call c on c.tx_hash = e.evt_tx_hash
        )


,p3_add_rn as (select (max(case when purchase_method = 'Offer Accepted' and sub_type = 'offer' and sub_idx = 0 then token_contract_address::string
                     when purchase_method = 'Buy Now' and sub_type = 'consideration' then token_contract_address::string
                end) over (partition by tx_hash, evt_index)) as avg_original_currency_contract
          ,sum(case when purchase_method = 'Offer Accepted' and sub_type = 'offer' and sub_idx = 0 then original_amount
                    when purchase_method = 'Buy Now' and sub_type = 'consideration' then original_amount
               end) over (partition by tx_hash, evt_index)
           / nft_transfer_count as avg_original_amount
          ,sum(case when fee_royalty_yn = 'fee' then original_amount end) over (partition by tx_hash, evt_index) / nft_transfer_count as avg_fee_amount
          ,sum(case when fee_royalty_yn = 'royalty' then original_amount end) over (partition by tx_hash, evt_index) / nft_transfer_count as avg_royalty_amount
          ,(max(case when fee_royalty_yn = 'fee' then receiver::string end) over (partition by tx_hash, evt_index)) as avg_fee_receive_address
          ,(max(case when fee_royalty_yn = 'royalty' then receiver::string end) over (partition by tx_hash, evt_index)) as avg_royalty_receive_address
          ,a.*
      from (select case when purchase_method = 'Offer Accepted' and sub_type = 'consideration' and fee_royalty_idx = 1 then 'fee'
                        when purchase_method = 'Offer Accepted' and sub_type = 'consideration' and fee_royalty_idx = 2 then 'royalty'
                        when purchase_method = 'Buy Now' and sub_type = 'consideration' and fee_royalty_idx = 2 then 'fee'
                        when purchase_method = 'Buy Now' and sub_type = 'consideration' and fee_royalty_idx = 3 then 'royalty'
                   end as fee_royalty_yn
                  ,case when purchase_method = 'Offer Accepted' and main_type = 'order' then 'Individual Offer'
                        when purchase_method = 'Offer Accepted' and main_type = 'basic_order' then 'Individual Offer'
                        when purchase_method = 'Offer Accepted' and main_type = 'advanced_order' then 'Collection/Trait Offers'
                        else 'Buy Now'
                   end as order_type
                  ,a.*
              from (select count(case when item_type in ('2','3') then 1 end) over (partition by tx_hash, evt_index) as nft_transfer_count
                          ,sum(case when item_type in ('0','1') then 1 end) over (partition by tx_hash, evt_index, sub_type order by sub_idx) as fee_royalty_idx
                          ,case when max(case when (sub_type,sub_idx,item_type) in (('offer',0,'1')) then 1 else 0 end) over (partition by tx_hash) = 1 then 'Offer Accepted'
                                else 'Buy Now'
                           end as purchase_method
                          ,a.*
                      from p3_evt a
                    ) a
            ) a
)

,p3_txn_level as (
    select main_type
          ,sub_idx
          ,tx_hash
          ,block_time
          ,block_number
          ,zone
          ,exchange_contract_address
          ,evt_index
          ,order_type
          ,purchase_method
          ,receiver as buyer
          ,sender as seller
          ,avg_original_amount as original_amount
          ,avg_original_currency_contract as original_currency_contract
          ,avg_fee_receive_address as fee_receive_address
          ,avg_fee_amount as fee_amount
          ,avg_original_currency_contract as fee_currency_contract
          ,avg_royalty_receive_address as royalty_receive_address
          ,avg_royalty_amount as royalty_amount
          ,avg_original_currency_contract as royalty_currency_contract
          ,token_contract_address as nft_contract_address
          ,token_id as nft_token_id
          ,nft_transfer_count
          ,original_amount as nft_item_count
          ,coalesce(avg_original_amount,0) + coalesce(avg_fee_amount,0) + coalesce(avg_royalty_amount,0) as attempt_amount
          ,0 as revert_amount
          ,false as reverted
          ,case when nft_transfer_count > 1 then true else false end as price_estimated
          ,'' as offer_order_type
          ,item_type
          ,order_type_id
      from p3_add_rn a
     where item_type in ('2','3')
)

,p3_seaport_transfers as (select
          'ethereum' as blockchain
          ,'seaport' as project
          ,'v1' as version
          ,TRY_CAST(date_trunc('DAY', a.block_time) AS date) AS block_date
          ,a.block_time
          ,a.block_number
          ,a.nft_token_id as token_id
          ,n.name as collection
          ,a.attempt_amount / power(10,t1.decimals) * p1.price as amount_usd
          ,case when item_type = '2' then 'erc721'
                when item_type = '3' then 'erc1155'
           end as token_standard
          ,case when order_type = 'Bulk Purchase' then 'Bulk Purchase'
                when nft_transfer_count = 1 then 'Single Item Trade'
                else 'Bundle Trade'
           end as trade_type
          ,nft_transfer_count as number_of_items
          ,a.purchase_method as trade_category
          ,'Trade' as evt_type
          ,concat('0x',substr(seller,3,40)) as seller
          ,concat('0x',substr(buyer,3,40)) as buyer
          ,a.attempt_amount / power(10,t1.decimals) as amount_original
          ,a.attempt_amount as amount_raw
          ,case when a.original_currency_contract = '0x0000000000000000000000000000000000000000' then 'ETH'
                else t1.symbol
           end as currency_symbol
          ,case when a.original_currency_contract = '0x0000000000000000000000000000000000000000' then
          '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
                else a.original_currency_contract
           end as currency_contract
          ,nft_contract_address
          ,a.exchange_contract_address as project_contract_address
          ,agg.name as aggregator_name
          ,agg.contract_address as aggregator_address
          ,a.tx_hash
          ,tx.from as tx_from
          ,tx.to as tx_to
          ,ROUND((2.5*(a.attempt_amount)/100),7) AS platform_fee_amount_raw
          ,ROUND((2.5*((a.attempt_amount / power(10,t1.decimals)))/100),7) AS platform_fee_amount
          ,ROUND((2.5*((a.attempt_amount / power(10,t1.decimals)* p1.price))/100),7) AS platform_fee_amount_usd
          ,'2.5' as platform_fee_percentage
          ,a.royalty_amount as royalty_fee_amount_raw
          ,a.royalty_amount / power(10,t1.decimals) as royalty_fee_amount
          ,a.royalty_amount / power(10,t1.decimals) * p1.price as royalty_fee_amount_usd
          ,(a.royalty_amount / a.attempt_amount * 100)::string  AS royalty_fee_percentage
          ,case when royalty_amount > 0 then royalty_receive_address end as
          royalty_fee_receive_address
          ,case when royalty_amount > 0 and a.original_currency_contract =
          '0x0000000000000000000000000000000000000000' then 'ETH'
          when royalty_amount > 0 then t1.symbol
          end as royalty_fee_currency_symbol
          ,a.tx_hash || '-' || a.attempt_amount::string || '-' || a.nft_token_id || '-' ||  concat('0x',substr(seller,3,40)) || '-' ||
          order_type_id::string || '-' || cast(row_number () over (partition by tx_hash order by sub_idx) as
          string) as unique_trade_id,
          a.zone
      from p3_txn_level a
        left join {{ source('ethereum','transactions') }} tx
            on tx.hash = a.tx_hash
            {% if not is_incremental() %}
            and tx.block_number > 14801608
            {% endif %}
            {% if is_incremental() %}
            and tx.block_time >= date_trunc("day", now() - interval '1 week')
            {% endif %}
        left join {{ ref('nft_ethereum_aggregators') }} agg
            ON agg.contract_address = tx.to
        left join {{ ref('tokens_ethereum_nft') }} n
            on n.contract_address = nft_contract_address
        left join {{ ref('tokens_ethereum_erc20') }} t1
            on t1.contract_address =
                case when a.original_currency_contract = '0x0000000000000000000000000000000000000000'
                then '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
                else a.original_currency_contract
                end
          left join {{ source('prices', 'usd') }} p1
            on p1.contract_address =
                case when a.original_currency_contract = '0x0000000000000000000000000000000000000000'
                then '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
                else a.original_currency_contract
                end
            and p1.minute = date_trunc('minute', a.block_time)
            and p1.blockchain = 'ethereum'
            {% if is_incremental() %}
            and p1.minute >= date_trunc("day", now() - interval '1 week')
            {% endif %}
            )

,p4_call as (select 'match_orders' as main_type
          ,'match_orders' as sub_type
          ,idx as sub_idx
          ,get_json_object(get_json_object(c.orders[0], "$.parameters"), "$.zone") as zone
          ,get_json_object(each, "$.offerer") as offerer
          ,get_json_object(get_json_object(each, "$.item"),"$.token") as offer_token
          ,get_json_object(get_json_object(each, "$.item"),"$.amount") as offer_amount
          ,get_json_object(get_json_object(each, "$.item"),"$.itemType") as offer_item_type
          ,get_json_object(get_json_object(each, "$.item"),"$.identifier") as offer_identifier
          ,get_json_object(get_json_object(each, "$.item"),"$.recipient") as recipient
          ,c.call_tx_hash as tx_hash
          ,c.call_block_time as block_time
          ,c.call_block_number as block_number
          ,c.contract_address as exchange_contract_address
     from (select *, posexplode(output_executions) as (idx, each) from {{ source('seaport_ethereum','Seaport_call_matchOrders') }}
     {% if is_incremental() %} -- this filter will only be applied on an incremental run
     where call_block_time >= (select max(block_time) from {{ this }})
     {% endif %}
     ) c
    where call_success

    union all
    select 'match_advanced_orders' as main_type
          ,'match_advanced_orders' as sub_type
          ,idx as sub_idx
          ,get_json_object(get_json_object(c.advancedOrders[0], "$.parameters"), "$.zone") as zone
          ,get_json_object(each, "$.offerer") as offerer
          ,get_json_object(get_json_object(each, "$.item"),"$.token") as offer_token
          ,get_json_object(get_json_object(each, "$.item"),"$.amount") as offer_amount
          ,get_json_object(get_json_object(each, "$.item"),"$.itemType") as offer_item_type
          ,get_json_object(get_json_object(each, "$.item"),"$.identifier") as offer_identifier
          ,get_json_object(get_json_object(each, "$.item"),"$.recipient") as recipient
          ,c.call_tx_hash as tx_hash
          ,c.call_block_time as block_time
          ,c.call_block_number as block_number
          ,c.contract_address as exchange_contract_address
    from (select *, posexplode(output_executions) as (idx, each) from {{ source('seaport_ethereum','Seaport_call_matchAdvancedOrders') }}
    {% if is_incremental() %} -- this filter will only be applied on an incremental run
    where call_block_time >= (select max(block_time) from {{ this }})
    {% endif %}
    ) c
    where call_success)


  ,p4_add_rn as (
    select max(case when fee_royalty_yn = 'price' then offerer end) over (partition by tx_hash) as price_offerer
          ,max(case when fee_royalty_yn = 'price' then recipient end) over (partition by tx_hash) as price_recipient
          ,max(case when fee_royalty_yn = 'price' then offer_token end) over (partition by tx_hash) as price_token
          ,max(case when fee_royalty_yn = 'price' then offer_amount end) over (partition by tx_hash) / nft_transfer_count as price_amount
          ,max(case when fee_royalty_yn = 'price' then offer_item_type end) over (partition by tx_hash) as price_item_type
          ,max(case when fee_royalty_yn = 'price' then offer_identifier end) over (partition by tx_hash) as price_id
          ,max(case when fee_royalty_yn = 'fee' then offerer end) over (partition by tx_hash) as fee_offerer
          ,max(case when fee_royalty_yn = 'fee' then recipient end) over (partition by tx_hash) as fee_recipient
          ,max(case when fee_royalty_yn = 'fee' then offer_token end) over (partition by tx_hash) as fee_token
          ,max(case when fee_royalty_yn = 'fee' then offer_amount end) over (partition by tx_hash) / nft_transfer_count as fee_amount
          ,max(case when fee_royalty_yn = 'fee' then offer_item_type end) over (partition by tx_hash) as fee_item_type
          ,max(case when fee_royalty_yn = 'fee' then offer_identifier end) over (partition by tx_hash) as fee_id
          ,max(case when fee_royalty_yn = 'royalty' then offerer end) over (partition by tx_hash) as royalty_offerer
          ,max(case when fee_royalty_yn = 'royalty' then recipient end) over (partition by tx_hash) as royalty_recipient
          ,max(case when fee_royalty_yn = 'royalty' then offer_token end) over (partition by tx_hash) as royalty_token
          ,max(case when fee_royalty_yn = 'royalty' then offer_amount end) over (partition by tx_hash) / nft_transfer_count as royalty_amount
          ,max(case when fee_royalty_yn = 'royalty' then offer_item_type end) over (partition by tx_hash) as royalty_item_type
          ,max(case when fee_royalty_yn = 'royalty' then offer_identifier end) over (partition by tx_hash) as royalty_id
          ,a.*
      from (select case when fee_royalty_idx = 1 then 'price'
                        when fee_royalty_idx = 2 then 'fee'
                        when fee_royalty_idx = 3 then 'royalty'
                   end as fee_royalty_yn
                  ,a.*
              from (select count(case when offer_item_type in ('2','3') then 1 end) over (partition by tx_hash) as nft_transfer_count
                          ,sum(case when offer_item_type in ('0','1') then 1 end) over (partition by tx_hash order by sub_idx) as fee_royalty_idx
                          ,a.*
                      from p4_call a
                    ) a
             where nft_transfer_count > 0  -- some of trades without NFT happens in match_order
            ) a
)

,p4_transfer_level as (
    select a.main_type
          ,a.sub_idx
          ,a.tx_hash
          ,a.block_time
          ,a.block_number
          ,a.zone
          ,a.exchange_contract_address
          ,offer_token as nft_address
          ,offer_identifier as nft_token_id
          ,recipient as buyer
          ,offerer as seller
          ,offer_item_type as offer_item_type
          ,offer_identifier as nft_token_id_dcnt
          ,offer_amount as nft_token_amount
          ,price_token as price_token
          ,price_item_type as price_item_type
          ,price_amount as price_amount
          ,fee_amount as fee_amount
          ,royalty_amount as royalty_amount
          ,price_amount as evt_price_amount
          ,fee_amount as evt_fee_amount
          ,royalty_amount as evt_royalty_amount
          ,fee_token as evt_fee_token
          ,royalty_token as evt_royalty_token
          ,fee_recipient as evt_fee_recipient
          ,royalty_recipient as evt_royalty_recipient
          ,coalesce(price_amount,0) + coalesce(fee_amount,0) + coalesce(royalty_amount,0) as attempt_amount
          ,0 as revert_amount
          ,false as reverted
          ,'' as offer_order_type
          ,'Private Sales' as order_type
          ,'Buy Now' as purchase_method
          ,nft_transfer_count
      from p4_add_rn a
     where offer_item_type in ('2','3')
)

,p4_seaport_transfers as (
          select
          'ethereum' as blockchain
          ,'seaport' as project
          ,'v1' as version
          ,TRY_CAST(date_trunc('DAY', a.block_time) AS date) AS block_date
          ,a.block_time
          ,a.block_number
          ,a.nft_token_id as token_id
          ,n.name as collection
          ,a.attempt_amount / power(10,t1.decimals) * p1.price as amount_usd
          ,case when offer_item_type = '2' then 'erc721'
                when offer_item_type = '3' then 'erc1155'
           end as token_standard
          ,case when order_type = 'Bulk Purchase' then 'Bulk Purchase'
                when nft_transfer_count = 1 then 'Single Item Trade'
                else 'Bundle Trade'
           end as trade_type
          ,nft_token_amount as number_of_items
          ,a.purchase_method as trade_category
          ,'Trade' as evt_type
          ,concat('0x',substr(seller,3,40)) as seller
          ,concat('0x',substr(buyer,3,40)) as buyer
          ,a.attempt_amount / power(10,t1.decimals) as amount_original
          ,a.attempt_amount as amount_raw
          ,case when concat('0x',substr(a.price_token,3,40)) =
          '0x0000000000000000000000000000000000000000' then 'ETH'
                else t1.symbol
           end as currency_symbol
          ,case when concat('0x',substr(a.price_token,3,40)) =
          '0x0000000000000000000000000000000000000000' then '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
                else concat('0x',substr(a.price_token,3,40))
           end as currency_contract
          ,concat('0x',substr(a.nft_address,3,40)) as nft_contract_address
          ,a.exchange_contract_address as project_contract_address
          ,agg.name as aggregator_name
          ,agg.contract_address as aggregator_address
          ,a.tx_hash
          ,tx.from as tx_from
          ,tx.to as tx_to
          ,ROUND((2.5*(a.attempt_amount)/100),7) AS platform_fee_amount_raw
          ,ROUND((2.5*((a.attempt_amount / power(10,t1.decimals)))/100),7) AS platform_fee_amount
          ,ROUND((2.5*((a.attempt_amount / power(10,t1.decimals)* p1.price))/100),7) AS platform_fee_amount_usd
          ,'2.5' as platform_fee_percentage
          ,a.evt_royalty_amount as royalty_fee_amount_raw
          ,a.evt_royalty_amount / power(10,t1.decimals) as royalty_fee_amount
          ,a.evt_royalty_amount / power(10,t1.decimals) * p1.price as royalty_fee_amount_usd
          ,(a.evt_royalty_amount / a.attempt_amount * 100)::string  AS royalty_fee_percentage
          ,case when evt_royalty_amount > 0 then concat('0x',substr(evt_royalty_recipient,3,40)) end as
          royalty_fee_receive_address
          ,case when evt_royalty_amount > 0 and concat('0x',substr(a.evt_royalty_token,3,40)) =
          '0x0000000000000000000000000000000000000000' then 'ETH'
                when evt_royalty_amount > 0 then t1.symbol
          end as royalty_fee_currency_symbol
          ,a.tx_hash || '-' || a.nft_token_id || '-' || a.attempt_amount::string || '-' || concat('0x',substr(seller,3,40)) || '-' || cast(row_number () over (partition by tx_hash order by sub_idx) as
          string) as unique_trade_id,
          a.zone
    from p4_transfer_level a
    left join {{ source('ethereum','transactions') }} tx
        on tx.hash = a.tx_hash
        {% if not is_incremental() %}
        and tx.block_number > 14801608
        {% endif %}
        {% if is_incremental() %}
        and tx.block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
    left join {{ ref('nft_ethereum_aggregators') }} agg
        ON agg.contract_address = tx.to
    left join {{ ref('tokens_ethereum_nft') }} n
        on n.contract_address = concat('0x',substr(a.nft_address,3,40))
    left join {{ ref('tokens_ethereum_erc20') }} t1
        on t1.contract_address =
            case when concat('0x',substr(a.price_token,3,40)) = '0x0000000000000000000000000000000000000000'
            then '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
            else concat('0x',substr(a.price_token,3,40))
            end
        left join {{ source('prices', 'usd') }} p1
        on p1.contract_address =
            case when concat('0x',substr(a.price_token,3,40)) = '0x0000000000000000000000000000000000000000'
            then '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
            else concat('0x',substr(a.price_token,3,40))
            end
        and p1.minute = date_trunc('minute', a.block_time)
        and p1.blockchain = 'ethereum'
        {% if is_incremental() %}
        and p1.minute >= date_trunc("day", now() - interval '1 week')
        {% endif %}
            )

select * from p1_seaport_transfers
    union all
select *
      from p2_seaport_transfers
    union all
select *
      from p3_seaport_transfers
    union all
select *
      from p4_seaport_transfers