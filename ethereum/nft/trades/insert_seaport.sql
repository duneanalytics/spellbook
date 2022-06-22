CREATE OR REPLACE FUNCTION nft.insert_seaport(start_ts timestamptz, end_ts timestamptz=now(), start_block numeric=0, end_block numeric=9e18) RETURNS integer
LANGUAGE plpgsql AS $function$
DECLARE r integer;
BEGIN

with iv_transfer_level as (
    select 'normal' as main_type
          ,'offer' as sub_type
          ,offer_idx as sub_idx
          ,offerer as sender
          ,recipient as receiver
          ,zone
          ,concat('\x',substr(offer2->>'token',3,40))::bytea as token_contract_address
          ,(offer2->>'amount')::numeric as original_amount
          ,offer2->>'itemType' as item_type
          ,(offer2->>'identifier') as token_id
          ,contract_address as exchange_contract_address
          ,evt_tx_hash as tx_hash
          ,evt_block_time as block_time
          ,evt_block_number as block_number
          ,evt_index
      from seaport."Seaport_evt_OrderFulfilled" a
          ,jsonb_array_elements(offer) with ordinality as t (offer2, offer_idx)
     where 1=1
       and recipient != '\x0000000000000000000000000000000000000000'::bytea
       and zone in ('\xf397619df7bfd4d1657ea9bdd9df7ff888731a11'::bytea
                   ,'\x9b814233894cd227f561b78cc65891aa55c62ad2'::bytea
                   ,'\x004c00500000ad104d7dbd00e3ae0a5c00560c00'::bytea
                   )
    union all
    select 'normal' as main_type 
          ,'consideration' as sub_type
          ,consideration_idx as sub_idx
          ,recipient as sender 
          ,concat('\x',substr(consideration2->>'recipient',3,40))::bytea as receiver
          ,zone 
          ,concat('\x',substr(consideration2->>'token',3,40))::bytea as token_contract_address 
          ,(consideration2->>'amount')::numeric as original_amount 
          ,consideration2->>'itemType' as item_type 
          ,(consideration2->>'identifier') as token_id
          ,contract_address as exchange_contract_address
          ,evt_tx_hash as tx_hash 
          ,evt_block_time as block_time
          ,evt_block_number as block_number 
          ,evt_index
      from seaport."Seaport_evt_OrderFulfilled" a
          ,jsonb_array_elements(consideration) with ordinality as t (consideration2, consideration_idx)
     where 1=1
       and recipient != '\x0000000000000000000000000000000000000000'::bytea     
       and zone in ('\xf397619df7bfd4d1657ea9bdd9df7ff888731a11'::bytea
                   ,'\x9b814233894cd227f561b78cc65891aa55c62ad2'::bytea
                   ,'\x004c00500000ad104d7dbd00e3ae0a5c00560c00'::bytea
                   )
    union all
    select 'private' as main_type
          ,'mix' as sub_type 
          ,a.consideration_idx as sub_idx
          ,e.offerer as sender
          ,concat('\x',substr(a.consideration2->>'recipient',3,40))::bytea as receiver
          ,a.zone
          ,concat('\x',substr(a.consideration2->>'token',3,40))::bytea as token_contract_address
          ,(a.consideration2->>'amount')::numeric as original_amount
          ,a.consideration2->>'itemType' as item_type
          ,(a.consideration2->>'identifier') as token_id
          ,a.contract_address as exchange_contract_address
          ,a.evt_tx_hash as tx_hash
          ,a.evt_block_time as block_time
          ,a.evt_block_number as block_number
          ,a.evt_index
     from (select *
             from seaport."Seaport_evt_OrderFulfilled" a
                 ,jsonb_array_elements(a.consideration) with ordinality as c (consideration2, consideration_idx)
            where a.recipient = '\x0000000000000000000000000000000000000000'::bytea
              and a.zone in ('\xf397619df7bfd4d1657ea9bdd9df7ff888731a11'::bytea
                           ,'\x9b814233894cd227f561b78cc65891aa55c62ad2'::bytea
                           ,'\x004c00500000ad104d7dbd00e3ae0a5c00560c00'::bytea
                           )
           ) a
          inner join (select *
                         from seaport."Seaport_evt_OrderFulfilled" b
                             ,jsonb_array_elements(b.offer) with ordinality as d (offer2, offer_idx)
                        where b.recipient = '\x0000000000000000000000000000000000000000'::bytea
                      ) e on a.recipient = e.recipient 
                          and a.evt_tx_hash = e.evt_tx_hash
                          and a.consideration2->>'token' = e.offer2->>'token'
                          and a.consideration2->>'itemType' = e.offer2->>'itemType'
                          and a.consideration2->>'identifier' = e.offer2->>'identifier'
                          and e.zone in ('\xf397619df7bfd4d1657ea9bdd9df7ff888731a11'::bytea
                                       ,'\x9b814233894cd227f561b78cc65891aa55c62ad2'::bytea
                                       ,'\x004c00500000ad104d7dbd00e3ae0a5c00560c00'::bytea
                                       )
)
,iv_txn_level as (
    select tx_hash
          ,block_time
          ,block_number
          ,0 as evt_index
          ,category
          ,exchange_contract_address
          ,zone
          ,max(case when item_type in ('2','3') then sender::text end)::bytea as seller
          ,max(case when item_type in ('2','3') then receiver::text end)::bytea as buyer
          ,sum(case when category = 'auction' and sub_idx in (1,2) then original_amount
                    when category = 'offer accepted' and sub_type = 'offer' and sub_idx = 1 then original_amount
                    when category = 'click buy now' and sub_type = 'consideration' then original_amount
               end) as original_amount
          ,max(case when category = 'auction' and sub_idx in (1,2) then token_contract_address::text
                    when category = 'offer accepted' and sub_type = 'offer' and sub_idx = 1 then token_contract_address::text
                    when category = 'click buy now' and sub_type = 'consideration' then token_contract_address::text
               end)::bytea as original_currency_contract
          ,case when max(case when category = 'auction' and sub_idx in (1,2) then token_contract_address::text
                            when category = 'offer accepted' and sub_type = 'offer' and sub_idx = 1 then token_contract_address::text
                            when category = 'click buy now' and sub_type = 'consideration' then token_contract_address::text
                       end)::bytea = '\x0000000000000000000000000000000000000000'::bytea
                then '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
                else max(case when category = 'auction' and sub_idx in (1,2) then token_contract_address::text
                            when category = 'offer accepted' and sub_type = 'offer' and sub_idx = 1 then token_contract_address::text
                            when category = 'click buy now' and sub_type = 'consideration' then token_contract_address::text
                       end)::bytea
            end as currency_contract
          ,max(case when category = 'auction' and sub_idx = 2 then receiver::text
                    when category = 'offer accepted' and sub_type = 'consideration' and item_type = '1' then receiver::text
                    when category = 'click buy now' and sub_type = 'consideration' and sub_idx = 2 then receiver::text
               end)::bytea as fee_receive_address
          ,sum(case when category = 'auction' and sub_idx = 2 then original_amount
                    when category = 'offer accepted' and sub_type = 'consideration' and item_type = '1' then original_amount
                    when category = 'click buy now' and sub_type = 'consideration' and sub_idx = 2 then original_amount
               end) as fee_amount
          ,max(case when category = 'auction' and sub_idx = 2 then token_contract_address::text
                    when category = 'offer accepted' and sub_type = 'consideration' and item_type = '1' then token_contract_address::text
                    when category = 'click buy now' and sub_type = 'consideration' and sub_idx = 2 then token_contract_address::text
               end)::bytea as fee_currency_contract
          ,case when max(case when category = 'auction' and sub_idx = 2 then token_contract_address::text
                            when category = 'offer accepted' and sub_type = 'consideration' and item_type = '1' then token_contract_address::text
                            when category = 'click buy now' and sub_type = 'consideration' and sub_idx = 2 then token_contract_address::text
                       end)::bytea = '\x0000000000000000000000000000000000000000'::bytea
                then '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
                else max(case when category = 'auction' and sub_idx = 2 then token_contract_address::text
                            when category = 'offer accepted' and sub_type = 'consideration' and item_type = '1' then token_contract_address::text
                            when category = 'click buy now' and sub_type = 'consideration' and sub_idx = 2 then token_contract_address::text
                       end)::bytea
           end as currency_contract2
          ,max(case when nft_transfer_count = 1 and item_type in ('2','3') then token_contract_address::text 
                    else token_contract_address::text 
               end)::bytea as nft_contract_address
          ,max(case when nft_transfer_count = 1 and item_type in ('2','3') then token_id
                    else token_id::text 
               end) as nft_token_id
          ,count(case when item_type = '2' then 1 end) as erc721_transfer_count
          ,count(case when item_type = '3' then 1 end) as erc1155_transfer_count
          ,count(case when item_type in ('2','3') then 1 end) as nft_transfer_count
          ,coalesce(sum(case when item_type = '2' then original_amount end),0) as erc721_item_count
          ,coalesce(sum(case when item_type = '3' then original_amount end),0) as erc1155_item_count
          ,coalesce(sum(case when item_type in ('2','3') then original_amount end),0) as nft_item_count 
      from (
            select a.*
                  ,count(case when item_type in ('2','3') then 1 end) over (partition by tx_hash, evt_index) as nft_transfer_count
                  ,case when main_type = 'private' then 'auction'
                        when max(case when item_type in ('0','1') then item_type end) over (partition by tx_hash, evt_index) = '0' then 'click buy now' 
                        else 'offer accepted' 
                   end as category
                  ,case when (item_type, sub_idx) in (('2',1),('3',1)) then True
                        when main_type = 'private' and sub_idx = 3 then True 
                   end as first_item
              from iv_transfer_level a
            ) a
     group by 1,2,3,4,5,6,7
)
,iv_nft_trades as (
    select a.block_time
          ,n.name as nft_project_name
          ,nft_token_id
          ,case when erc721_transfer_count > 0 and erc1155_transfer_count = 0 then 'erc721'
                when erc721_transfer_count = 0 and erc1155_transfer_count > 0 then 'erc1155'
                when erc721_transfer_count > 0 and erc1155_transfer_count > 0 then 'mixed'
           end as erc_standard
          ,case when a.zone in ('\xf397619df7bfd4d1657ea9bdd9df7ff888731a11'
                               ,'\x9b814233894cd227f561b78cc65891aa55c62ad2'
                               ,'\x004c00500000ad104d7dbd00e3ae0a5c00560c00'
                               )
                then 'OpenSea'
           end as platform
          ,case when a.zone in ('\xf397619df7bfd4d1657ea9bdd9df7ff888731a11'
                               ,'\x9b814233894cd227f561b78cc65891aa55c62ad2'
                               ,'\x004c00500000ad104d7dbd00e3ae0a5c00560c00'
                               )
                then '3'
           end as platform_version
          ,case when spc4.call_tx_hash is not null then 'Bulk Purchase'
                when spc5.call_tx_hash is not null then 'Bulk Purchase'
                when nft_transfer_count = 1 then 'Single Item Trade'
                else 'Bundle Trade'
           end as trade_type
          ,nft_item_count as number_of_items
          ,case when category = 'auction' then 'Buy'
                when category = 'click buy now' then 'Buy'
                when category = 'offer accepted' then 'Offer Accepted'
                else 'Buy'
           end as category
          ,'Trade' as evt_type
          ,a.original_amount / 10^t1.decimals * p1.price as usd_amount
          ,seller
          ,buyer
          ,a.original_amount / 10^t1.decimals as original_amount
          ,a.original_amount as original_amount_raw
          ,case when a.original_currency_contract = '\x0000000000000000000000000000000000000000'::bytea then 'ETH'
                else t1.symbol
           end as original_currency
          ,a.original_currency_contract
          ,a.currency_contract
          ,a.nft_contract_address
          ,a.exchange_contract_address
          ,a.tx_hash
          ,a.block_number
          ,tx."from" as tx_from
          ,tx."to" as tx_to
          ,a.evt_index
          ,1 as trade_id
          ,a.fee_receive_address
          ,case when a.fee_currency_contract = '\x0000000000000000000000000000000000000000'::bytea then 'ETH'
                else t2.symbol 
           end as fee_currency
          ,a.fee_amount as fee_amount_raw
          ,a.fee_amount / 10^t2.decimals as fee_amount
          ,a.fee_amount / 10^t2.decimals * p2.price as fee_usd_amount
          ,a.zone as zone_address
      from iv_txn_level a
           left join ethereum.transactions tx on tx.hash = a.tx_hash
                                              and tx.block_number > 14801608
           left join nft.tokens n on n.contract_address = a.nft_contract_address
           left join prices.usd p1 on p1.contract_address = a.currency_contract
                                   and p1.minute = date_trunc('minute', a.block_time)
                                   and p1.minute >= '2022-05-15'
           left join erc20.tokens t1 on t1.contract_address = a.currency_contract
           left join prices.usd p2 on p2.contract_address = a.currency_contract2
                                   and p2.minute = date_trunc('minute', a.block_time)
                                   and p2.minute >= '2022-05-15'
           left join erc20.tokens t2 on t2.contract_address = a.currency_contract2
           left join seaport."Seaport_call_fulfillAvailableAdvancedOrders" spc4 on spc4.call_tx_hash = a.tx_hash
           left join seaport."Seaport_call_fulfillAvailableOrders" spc5 on spc5.call_tx_hash = a.tx_hash
)
,rows AS (
    INSERT INTO nft.trades (
            block_time,
            nft_project_name,
            nft_token_id,
            erc_standard,
            platform,
            platform_version,
            trade_type,
            number_of_items,
            category,
            evt_type,
            usd_amount,
            seller,
            buyer,
            original_amount,
            original_amount_raw,
            original_currency,
            original_currency_contract,
            currency_contract,
            nft_contract_address,
            exchange_contract_address,
            tx_hash,
            block_number,
            nft_token_ids_array,
            senders_array,
            recipients_array,
            erc_types_array,
            nft_contract_addresses_array,
            erc_values_array,
            tx_from,
            tx_to,
            trace_address,
            evt_index,
            trade_id
    )
    select block_time 
            ,nft_project_name
            ,nft_token_id 
            ,erc_standard
            ,platform
            ,platform_version  
            ,trade_type
            ,number_of_items 
            ,category
            ,evt_type
            ,usd_amount 
            ,seller
            ,buyer
            ,original_amount
            ,original_amount_raw
            ,original_currency
            ,original_currency_contract
            ,currency_contract 
            ,nft_contract_address
            ,exchange_contract_address
            ,tx_hash  
            ,block_number
            ,NULL::text[] as nft_token_ids_array
            ,NULL::bytea[] as senders_array
            ,NULL::bytea[] as recipients_array
            ,NULL::text[] as erc_types_array
            ,NULL::bytea[] as nft_contract_addresses_array
            ,NULL::numeric[] as erc_values_array
            ,tx_from 
            ,tx_to
            ,NULL::numeric[] as trace_address
            ,evt_index
            ,ROW_NUMBER() OVER (PARTITION BY tx_hash ORDER BY evt_index) AS trade_id
      from iv_nft_trades
     where 1=1
       and block_time >= start_ts
       and block_time < end_ts
    ON CONFLICT DO NOTHING
    RETURNING 1
)
SELECT count(*) INTO r from rows;
RETURN r;
END
$function$;

-- fill 2022
SELECT nft.insert_seaport(
    '2022-06-10'
    , NOW()
    , (SELECT MAX(number) FROM ethereum.blocks WHERE time < '2022-06-10')
    , (SELECT MAX(number) FROM ethereum.blocks WHERE time < NOW() - interval '20 minutes')
)
WHERE NOT EXISTS (
    SELECT *
    FROM nft.trades
    WHERE block_time > '2022-06-10'
    AND block_time <= NOW() - interval '20 minutes'
    AND platform = 'OpenSea'
    AND platform_version = '3'
);

INSERT INTO cron.job (schedule, command)
VALUES ('/15 * * * *', $$
    SELECT nft.insert_seaport(
        (SELECT MAX(block_time) - interval '6 hours' FROM nft.trades WHERE platform='OpenSea' AND platform_version = '3')
        , (SELECT NOW() - interval '20 minutes')
        , (SELECT MAX(number) FROM ethereum.blocks WHERE time < (SELECT max(block_time) - interval '6 hours' FROM nft.trades WHERE platform='OpenSea' AND platform_version = '3'))
        , (SELECT MAX(number) FROM ethereum.blocks WHERE time < NOW() - interval '20 minutes'));
$$)
ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule; 
