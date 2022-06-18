-- welcome to the commented version of the Seaport data abstraction on the Dune legacy.
-- this first section pulls data from seaport."Seaport_evt_OrderFulfilled", which shows all successful transactions that occur on Seaport
create schema if not exists seaport;

create or replace view seaport.view_transactions 
AS
with iv_availadv as (
    select 'avail' as order_type
          ,'avail' as sub_type
          ,exec_idx as sub_idx
          ,concat('\x',substr(exec->>'offerer',3,40))::bytea as sender 
          ,concat('\x',substr(exec->'item'->>'recipient',3,40))::bytea as receiver
          ,concat('\x',substr("advancedOrders"->0->'parameters'->>'zone',3,40))::bytea as zone   -- assume that zone is unique per transaction
          ,concat('\x',substr(exec->'item'->>'token',3,40))::bytea as token_contract_address
          ,(exec->'item'->>'amount')::numeric as original_amount
          ,(exec->'item'->>'itemType') as item_type
          ,(exec->'item'->>'identifier') as token_id
          ,contract_address as exchange_contract_address
          ,call_tx_hash as tx_hash
          ,call_block_time as block_time
          ,call_block_number as block_number
          ,exec_idx as evt_index -- align to another dataset with unique value
      from seaport."Seaport_call_fulfillAvailableAdvancedOrders" a
          ,jsonb_array_elements(output_executions) with ordinality as t (exec, exec_idx)
     where call_success
)
,iv_transfer_level_pre as ( -- in this section, we extract and then union all offers and considerations from all successful transactions on Seaport and organize it into the same table structure, as well as pull out all the variables we need to later on to differentiate by order type
    select 'normal' as main_type -- this pulls data from normal (non private) orders that are successfully fulfilled. 
          ,'offer' as sub_type  -- Seaport includes both offers and considerations. These are ingested as json arrays into Dune. This section separates out the offer section of transactions that are succcessfully fulfilled. 
          ,offer_idx as sub_idx -- we'll use this sub index to help us find the final price by offer type
          ,offerer as sender -- wallet address of offerer (seller) that owns the nft and is offering it for sale
          ,recipient as receiver -- wallet address of fulfiller (buyer) that successfully completes the transaction
          ,zone -- A zone is an account (usually a contract) that performs additional validation prior to fulfillment, and that can cancel the listing on behalf of the offerer. We'll use this to select for OpenSea transactions on Seaport moving forward.
          ,concat('\x',substr(offer2->>'token',3,40))::bytea as token_contract_address -- contract address of the token requested by the offerer as payment (e.g.  0x0000000000000000000000000000000000000000 for ETH)
          ,(offer2->>'amount')::numeric as original_amount -- absolute amount of token requested by offerer as payment (e.g. 0.2 for 0.2E)
          ,offer2->>'itemType' as item_type -- we use this later on to properly identify wallet addresses and fee amounts
          ,(offer2->>'identifier') as token_id -- token # of the NFT being offered - e.g. for Doodle #1000, this reslt would be 1000
          ,contract_address as exchange_contract_address -- contract address of the exchange contract used - in this case, it is the Seaport exchange contract address 
          ,evt_tx_hash as tx_hash -- unique hash associated with this executed transaction on ETH
          ,evt_block_time as block_time -- time in UTC at which the block containing the transaction was executed
          ,evt_block_number as block_number -- number of the ETH block containing the transaction
          ,evt_index -- index of transaction within the block
      from seaport."Seaport_evt_OrderFulfilled" a -- this section explodes the json array of the "offer" section of the successful transaction in the OrderFulfilled table (which contains all successfully completed transactions)
          ,jsonb_array_elements(offer) with ordinality as t (offer2, offer_idx)
     where 1=1
       and recipient != '\x0000000000000000000000000000000000000000'::bytea
    --   and evt_tx_hash not in (select tx_hash from iv_availadv_txn)
    union all
    select 'normal' as main_type -- this pulls data from normal (non private) orders that are successfully fulfilled. 
          ,'consideration' as sub_type -- Seaport includes both offers and considerations. These are ingested as json arrays into Dune. This section separates out the consideration section of transactions that are succcessfully fulfilled. 
          ,consideration_idx as sub_idx -- we'll use this sub index to help us find the final price by offer type
          ,recipient as sender -- wallet address of fulfiller (buyer) that successfully completes the transaction
          ,concat('\x',substr(consideration2->>'recipient',3,40))::bytea as receiver
          ,zone -- A zone is an account (usually a contract) that performs additional validation prior to fulfillment, and that can cancel the listing on behalf of the offerer. We'll use this to select for OpenSea transactions on Seaport moving forward.
          ,concat('\x',substr(consideration2->>'token',3,40))::bytea as token_contract_address -- contract address of the token requested by the offerer as payment (e.g.  0x0000000000000000000000000000000000000000 for ETH)
          ,(consideration2->>'amount')::numeric as original_amount -- absolute amount of token requested by offerer as payment (e.g. 0.2 for 0.2E)
          ,consideration2->>'itemType' as item_type -- we use this later on to properly identify wallet addresses and fee amounts
          ,(consideration2->>'identifier') as token_id -- token # of the NFT being offered - e.g. for Doodle #1000, this reslt would be 1000
          ,contract_address as exchange_contract_address -- contract address of the exchange contract used - in this case, it is the Seaport exchange contract address 
          ,evt_tx_hash as tx_hash -- unique hash associated with this executed transaction on ETH
          ,evt_block_time as block_time -- time in UTC at which the block containing the transaction was executed
          ,evt_block_number as block_number -- number of the ETH block containing the transaction
          ,evt_index -- index of transaction within the block
      from seaport."Seaport_evt_OrderFulfilled" a -- this section explodes the json array of the "consideration" section of the successful transaction in the OrderFulfilled table (which contains all successfully completed transactions)
          ,jsonb_array_elements(consideration) with ordinality as t (consideration2, consideration_idx)
     where 1=1
       and recipient != '\x0000000000000000000000000000000000000000'::bytea     
    union all
    select 'private' as main_type -- this pulls data from private sales that are successfully fulfilled 
          ,'mix' as sub_type -- we'll pull data on private order offers + considerations together since the offerer actually specifies that only one wallet address may provide a consideration
          ,a.consideration_idx as sub_idx -- we'll use this sub index to help us find the final price by offer type
          ,e.offerer as sender -- wallet address of offerer (seller) that owns the nft and is offering it for sale
          ,concat('\x',substr(a.consideration2->>'recipient',3,40))::bytea as receiver -- wallet address of fulfiller (buyer) that successfully completes the transaction
          ,a.zone -- A zone is an account (usually a contract) that performs additional validation prior to fulfillment, and that can cancel the listing on behalf of the offerer. We'll use this to select for OpenSea transactions on Seaport moving forward.
          ,concat('\x',substr(a.consideration2->>'token',3,40))::bytea as token_contract_address -- contract address of the token requested by the offerer as payment (e.g.  0x0000000000000000000000000000000000000000 for ETH)
          ,(a.consideration2->>'amount')::numeric as original_amount  -- absolute amount of token requested by offerer as payment (e.g. 0.2 for 0.2E)
          ,a.consideration2->>'itemType' as item_type  -- we use this later on to properly identify wallet addresses and fee amounts
          ,(a.consideration2->>'identifier') as token_id -- token # of the NFT being offered - e.g. for Doodle #1000, this reslt would be 1000
          ,a.contract_address as exchange_contract_address -- contract address of the exchange contract used - in this case, it is the Seaport exchange contract address 
          ,a.evt_tx_hash as tx_hash -- unique hash associated with this executed transaction on ETH
          ,a.evt_block_time as block_time -- time in UTC at which the block containing the transaction was executed
          ,a.evt_block_number as block_number -- number of the ETH block containing the transaction
          ,a.evt_index -- index of transaction within the block
     from (select * -- this section explodes the json array of the "consideration" section of successful private sales through Seaport
             from seaport."Seaport_evt_OrderFulfilled" a
                 ,jsonb_array_elements(a.consideration) with ordinality as c (consideration2, consideration_idx)
            where a.recipient = '\x0000000000000000000000000000000000000000'::bytea  -- private sales have a fulfiller equal to null address
           ) a
          inner join (select *
                         from seaport."Seaport_evt_OrderFulfilled" b -- this section explodes the json array of the "offer" section of successful private sales through Seaport
                             ,jsonb_array_elements(b.offer) with ordinality as d (offer2, offer_idx)
                        where b.recipient = '\x0000000000000000000000000000000000000000'::bytea  -- private sales have a fulfiller equal to null address
                      ) e on a.recipient = e.recipient 
                          and a.evt_tx_hash = e.evt_tx_hash --join these based off unique, transaction level data including transaction hash
                          and a.consideration2->>'token' = e.offer2->>'token'
                          and a.consideration2->>'itemType' = e.offer2->>'itemType'
                          and a.consideration2->>'identifier' = e.offer2->>'identifier'
)
,iv_transfer_level as (
    select a.*
      from iv_transfer_level_pre a
           left join iv_availadv b on b.tx_hash = a.tx_hash
                                   and b.item_type in ('2','3')
                                   and b.token_contract_address = a.token_contract_address
                                   and b.token_id = a.token_id
                                   and b.sender = a.sender
                                   and b.receiver = a.receiver
           left join seaport."Seaport_call_fulfillAvailableAdvancedOrders" c on c.call_tx_hash = a.tx_hash
     where 1=1 
       and not (a.item_type in ('2','3') and b.tx_hash is null and c.call_tx_hash is not null)
)
,iv_txn_level as ( -- we now take all the unioned offers and considerations from successful transactions on Seaport and organize it into an unique, transaction level table (as each transaction has both an offer and a consideration), with some aggregations we need for later categorization by order type
    select tx_hash -- unique transaction hash 
          ,block_time -- time of transaction execution 
          ,block_number -- number of ETH block in which transaction was executed
          ,0 as evt_index -- index of transaction within the block
          ,category -- a derived column that contains information on the transaction order type - whether it was a private sale, auction, offer (and what type of offer), etc.
          ,exchange_contract_address -- contract address of the exchange contract used - in this case, it is the Seaport exchange contract address 
          ,zone -- A zone is an account (usually a contract) that performs additional validation prior to fulfillment, and that can cancel the listing on behalf of the offerer. We'll use this to select for OpenSea transactions on Seaport moving forward.
          ,max(case when item_type in ('2','3') then sender::text end)::bytea as seller -- let's extract seller wallet address in the same format regardless of order type
          ,max(case when item_type in ('2','3') then receiver::text end)::bytea as buyer -- and buyer wallet address too
          ,sum(case when category = 'auction' and sub_idx in (1,2) then original_amount
                    when category = 'offer accepted' and sub_type = 'offer' and sub_idx = 1 then original_amount
                    when category = 'click buy now' and sub_type = 'consideration' then original_amount
               end) as original_amount -- extract original amount (absolute amount of token requested by offerer as payment) into the same, uniform format regardless of order type
          ,max(case when category = 'auction' and sub_idx in (1,2) then token_contract_address::text
                    when category = 'offer accepted' and sub_type = 'offer' and sub_idx = 1 then token_contract_address::text
                    when category = 'click buy now' and sub_type = 'consideration' then token_contract_address::text
               end)::bytea as original_currency_contract -- extract contract address of the token(s) requested by the offerer as payment into the same, uniform format regardless of order type
          ,case when max(case when category = 'auction' and sub_idx in (1,2) then token_contract_address::text
                            when category = 'offer accepted' and sub_type = 'offer' and sub_idx = 1 then token_contract_address::text
                            when category = 'click buy now' and sub_type = 'consideration' then token_contract_address::text
                       end)::bytea = '\x0000000000000000000000000000000000000000'::bytea
                then '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
                else max(case when category = 'auction' and sub_idx in (1,2) then token_contract_address::text
                            when category = 'offer accepted' and sub_type = 'offer' and sub_idx = 1 then token_contract_address::text
                            when category = 'click buy now' and sub_type = 'consideration' then token_contract_address::text
                       end)::bytea
            end as currency_contract -- let's swap out ETH contract address for WETH contract address to make using the price table we later on use easier
          ,max(case when category = 'auction' and sub_idx = 2 then receiver::text
                    when category = 'offer accepted' and sub_type = 'consideration' and item_type = '1' then receiver::text
                    when category = 'click buy now' and sub_type = 'consideration' and sub_idx = 2 then receiver::text
               end)::bytea as fee_receive_address -- uniformly extract wallet addresses that receive fees in each transaction
          ,sum(case when category = 'auction' and sub_idx = 2 then original_amount
                    when category = 'offer accepted' and sub_type = 'consideration' and item_type = '1' then original_amount
                    when category = 'click buy now' and sub_type = 'consideration' and sub_idx = 2 then original_amount
               end) as fee_amount -- as well as the absolute amount of the fee(s)
          ,max(case when category = 'auction' and sub_idx = 2 then token_contract_address::text
                    when category = 'offer accepted' and sub_type = 'consideration' and item_type = '1' then token_contract_address::text
                    when category = 'click buy now' and sub_type = 'consideration' and sub_idx = 2 then token_contract_address::text
               end)::bytea as fee_currency_contract -- as well as the contract address of the token(s) paid in fees
          ,case when max(case when category = 'auction' and sub_idx = 2 then token_contract_address::text
                            when category = 'offer accepted' and sub_type = 'consideration' and item_type = '1' then token_contract_address::text
                            when category = 'click buy now' and sub_type = 'consideration' and sub_idx = 2 then token_contract_address::text
                       end)::bytea = '\x0000000000000000000000000000000000000000'::bytea
                then '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
                else max(case when category = 'auction' and sub_idx = 2 then token_contract_address::text
                            when category = 'offer accepted' and sub_type = 'consideration' and item_type = '1' then token_contract_address::text
                            when category = 'click buy now' and sub_type = 'consideration' and sub_idx = 2 then token_contract_address::text
                       end)::bytea
           end as currency_contract2 -- with the conversion of ETH into WETH to simplify USD price calculations later
          ,max(case when nft_transfer_count = 1 and item_type in ('2','3') then token_contract_address::text 
                    -- if this is a single NFT transaction, then give us the NFT contract address; if it's a bundle or multi-purchase, keep as null  (don't want to create array of all NFT contract addresses traded)
               end)::bytea as nft_contract_address
          ,max(case when nft_transfer_count = 1 and item_type in ('2','3') then token_id
                    -- if this is a single NFT transaction, then give us the NFT token ID; if it's a bundle or multi-purchase, keep as null (don't want to create array of all token IDs traded)
               end) as nft_token_id
          ,count(case when item_type = '2' then 1 end) as erc721_transfer_count -- number of ERC 721 tranfers in the transaction 
          ,count(case when item_type = '3' then 1 end) as erc1155_transfer_count -- number of ERC 1155 tranfers in the transaction 
          ,count(case when item_type in ('2','3') then 1 end) as nft_transfer_count -- total number of NFT (ERC-721 and ERC-1155) tranfers in the transaction
          ,coalesce(sum(case when item_type = '2' then original_amount end),0) as erc721_item_count  -- number of ERC 721 tokens transferred in the transaction 
          ,coalesce(sum(case when item_type = '3' then original_amount end),0) as erc1155_item_count -- number of ERC 1155 tokens transferred in the transaction 
          ,coalesce(sum(case when item_type in ('2','3') then original_amount end),0) as nft_item_count  -- total number of NFTs (ERC-721 and ERC-1155) transferred in the transaction
      from (
            select a.*
                  ,count(case when item_type in ('2','3') then 1 end) over (partition by tx_hash, evt_index) as nft_transfer_count -- count how many transfers of NFTs occurred in the transaction
                  ,case when main_type = 'private' then 'auction'
                        when max(case when item_type in ('0','1') then item_type end) over (partition by tx_hash, evt_index) = '0' then 'click buy now' 
                        else 'offer accepted' 
                   end as category -- identify the (general) type of order so we can use it to extract comparable original amount and wallet addresses above
                  ,case when (item_type, sub_idx) in (('2',1),('3',1)) then True
                        when main_type = 'private' and sub_idx = 3 then True 
                   end as first_item  -- identify the first item in a bundle/multiple NFT transaction
              from iv_transfer_level a
            ) a
     group by 1,2,3,4,5,6,7
)
,iv_nft_trades as ( -- now that we have an unique, transaction-level table let's adjust the formatting to make it mirror the well known Dune nft.trades view 
    select a.block_time -- time of the transaction in UTC (Ethereum is in UTC)
          ,n.name as nft_project_name -- name of the NFT collection, if we have it 
          ,nft_token_id -- the token ID of the NFT transacted, if there is one
          ,case when erc721_transfer_count > 0 and erc1155_transfer_count = 0 then 'erc721'
                when erc721_transfer_count = 0 and erc1155_transfer_count > 0 then 'erc1155'
                when erc721_transfer_count > 0 and erc1155_transfer_count > 0 then 'mixed'
           end as erc_standard -- whether the NFTs transacted are ERC-721, ERC-1155 or a mix of both
          ,case when a.zone in ('\xf397619df7bfd4d1657ea9bdd9df7ff888731a11'
                               ,'\x9b814233894cd227f561b78cc65891aa55c62ad2'
                               ,'\x004c00500000ad104d7dbd00e3ae0a5c00560c00'
                               )
                then 'OpenSea' -- use zone data to identify platform transacted
           end as platform -- platform on which transaction occurred (e.g. OpenSea)
          ,case when a.zone in ('\xf397619df7bfd4d1657ea9bdd9df7ff888731a11'
                               ,'\x9b814233894cd227f561b78cc65891aa55c62ad2'
                               ,'\x004c00500000ad104d7dbd00e3ae0a5c00560c00'
                               )
                then 3
           end as platform_version -- Seaport is the 3rd exchange contract used by OpenSea (after Wyvern 2.2 and Wyvern 2.3)
          ,case when nft_transfer_count = 1 then 'Single Item Trade'
                else 'Bundle Trade'
           end as trade_type -- identify whether it was a single NFT trade or multiple NFTs traded
          ,nft_item_count as number_of_items -- identify the number of items traded in the transaction
          ,'Trade' as evt_type -- identify these transactions as trades (there may be future transactions that are simply wallet-to-wallet transfers or swaps, for example)
          ,a.original_amount / 10^t1.decimals * p1.price as usd_amount -- use the prices table to convert the original amount to amount in USD at the minute of the transaction
          ,seller -- seller wallet address
          ,buyer -- buyer wallet address
          ,a.original_amount / 10^t1.decimals as original_amount -- original amount in original currency 
          ,a.original_amount as original_amount_raw -- raw original amount (can have many decimals or 0s in front)
          ,case when a.original_currency_contract = '\x0000000000000000000000000000000000000000'::bytea then 'ETH'
                else t1.symbol
           end as original_currency -- symbol of original token used in payment
          ,a.original_currency_contract -- contract address of original token used for payment
          ,a.currency_contract -- contract address of original token used for payment, with swap of ETH contract address for WETH 
          ,a.nft_contract_address -- nft contract address, if only 1 nft was transacted
          ,a.exchange_contract_address -- exchange contract address - in this case, Seaport contract address
          ,a.tx_hash -- unique transaction hash
          ,a.block_number -- number of the ETH block in which transaction was executed 
          ,tx."from" as tx_from -- actual "from" wallet address from ethereum.transactions table 
          ,tx."to" as tx_to -- actual "to" wallet address from ethereum.transactions table (can be different from actual buyer if an aggregator is used)
          ,a.evt_index -- actual "to" wallet address from ethereum.transactions table (can be different from actual buyer if an aggregator is used)
          ,1 as trade_id -- index of transaction within the block
          ,a.fee_receive_address -- wallet addresses receiving fees from the transaction
          ,case when a.fee_currency_contract = '\x0000000000000000000000000000000000000000'::bytea then 'ETH'
                else t2.symbol 
           end as fee_currency -- symbol of the token in which fees are paid out
          ,a.fee_amount as fee_amount_raw -- raw numerical amount of fees
          ,a.fee_amount / 10^t2.decimals as fee_amount -- fee amount in original token currency (properly formatted in decimals)
          ,a.fee_amount / 10^t2.decimals * p2.price as fee_usd_amount -- fee amount in USD
          ,a.zone as zone_address -- zone address, we use this to determine platform
          ,case when spc1.call_tx_hash is not null then 'Collection/Trait Offers' -- include English Auction and Dutch Auction
                when spc2.call_tx_hash is not null and (spc2.parameters->>'basicOrderType')::integer between 0 and 15 then 'Buy Now' -- Buy it directly
                when spc2.call_tx_hash is not null and (spc2.parameters->>'basicOrderType')::integer between 16 and 23 and (spc2.parameters->>'considerationIdentifier') = a.nft_token_id then 'Individual Offer'
                -- when spc2.call_tx_hash is not null and (spc2.parameters->>'basicOrderType')::integer between 16 and 23 then 'Collection/Trait Offer'  -- temporary
                when spc2.call_tx_hash is not null then 'Buy Now'
                when spc3.call_tx_hash is not null and a.original_currency_contract = '\x0000000000000000000000000000000000000000'::bytea then 'Buy Now'
                when spc3.call_tx_hash is not null then 'Collection/Trait Offers' -- offer for collection
                when spc4.call_tx_hash is not null then 'Bulk Purchase' -- bundles of NFTs are purchased through aggregators or in a cart 
                when spc5.call_tx_hash is not null then 'Bulk Purchase' -- bundles of NFTs are purchased through aggregators or in a cart 
                -- when spc3.call_tx_hash is not null and (spc3."advancedOrder" -> 'parameters' -> 'consideration' -> 0 ->> 'identifierOrCriteria') > '0' then 'Collection/Trait Offer' -- offer for specific criteria
                when spc6.call_tx_hash is not null then 'Private Sales' -- sales for designated address
                else 'Buy Now' -- temporary
           end as order_type  -- here we specify order type using the call tables as well as parameters passed into those (including basic order type and identifier criteria for collection and trait offers)
      from iv_txn_level a
           left join ethereum.transactions tx on tx.hash = a.tx_hash -- join in eth transactions table so that we can get to and from wallet address info
                                              and tx.block_number > 14801608
           left join nft.tokens n on n.contract_address = a.nft_contract_address
           left join prices.usd p1 on p1.contract_address = a.currency_contract  -- joining to prices table to allow us to get USD amount data
                                   and p1.minute = date_trunc('minute', a.block_time)
                                   and p1.minute >= '2022-05-15'
           left join erc20.tokens t1 on t1.contract_address = a.currency_contract
           left join prices.usd p2 on p2.contract_address = a.currency_contract2
                                   and p2.minute = date_trunc('minute', a.block_time)
                                   and p2.minute >= '2022-05-15'
           left join erc20.tokens t2 on t2.contract_address = a.currency_contract2
           left join seaport."Seaport_call_fulfillOrder" spc1 on spc1.call_tx_hash = a.tx_hash -- we uses these call tables to categorize transaction order type
           left join seaport."Seaport_call_fulfillBasicOrder" spc2 on spc2.call_tx_hash = a.tx_hash
           left join seaport."Seaport_call_fulfillAdvancedOrder" spc3 on spc3.call_tx_hash = a.tx_hash
           left join seaport."Seaport_call_fulfillAvailableAdvancedOrders" spc4 on spc4.call_tx_hash = a.tx_hash
           left join seaport."Seaport_call_fulfillAvailableOrders" spc5 on spc5.call_tx_hash = a.tx_hash
           left join seaport."Seaport_call_matchOrders" spc6 on spc6.call_tx_hash = a.tx_hash
)
 -- create the final table by taking most relevant columns from last table
select block_time -- time in UTC at which the block containing the transaction was executed (Ethereum is in UTC)
      ,nft_project_name -- name of the NFT collection, if we have it
      ,nft_token_id -- the token ID of the NFT transacted, if there is one
      ,erc_standard -- whether the NFTs transacted are ERC-721, ERC-1155 or a mix of both
      ,platform -- platform on which transaction occurred (e.g. OpenSea)
      ,platform_version  -- Seaport is the 3rd exchange contract used by OpenSea (after Wyvern 2.2 and Wyvern 2.3)
      ,trade_type -- identify whether it was a single NFT trade or multiple NFTs traded
      ,number_of_items -- identify the number of items traded in the transaction
      ,order_type  -- here we specify order type using the call tables as well as parameters passed into those (including basic order type and identifier criteria for collection and trait offers)
      ,evt_type -- identify these transactions as trades (there may be future transactions that are simply wallet-to-wallet transfers or swaps, for example)
      ,usd_amount -- use the prices table to convert the original amount to amount in USD at the minute of the transaction
      ,seller -- seller wallet address
      ,buyer -- buyer wallet address
      ,original_amount -- original amount in original currency (so 0.2 if original final price was 0.2E)
      ,original_amount_raw -- raw original amount (can have many decimals or 0s in front)
      ,original_currency -- symbol of original token used in payment
      ,original_currency_contract -- contract address of original token used for payment
      ,currency_contract -- contract address of original token used for payment, with ETH contract address swapped for WETH
      ,nft_contract_address -- nft contract address, if only 1 nft was transacted
      ,exchange_contract_address -- exchange contract address - in this case, Seaport contract address
      ,tx_hash  -- unique hash associated with this executed transaction on ETH
      ,block_number -- number of the ETH block in which transaction was executed 
      ,tx_from -- actual "from" wallet address from ethereum.transactions table 
      ,tx_to -- actual "to" wallet address from ethereum.transactions table (can be different from actual buyer if an aggregator is used)
      ,evt_index -- index of transaction within the block
      ,fee_receive_address  -- wallet addresses receiving fees from the transaction
      ,fee_currency -- symbol of the token in which fees are paid out
      ,fee_amount_raw -- raw numerical amount of fees
      ,fee_amount  -- fee amount in original token currency (properly formatted in decimals)
      ,fee_usd_amount -- fee amount in USD
      ,zone_address -- A zone is an account (usually a contract) that performs additional validation prior to fulfillment, and that can cancel the listing on behalf of the offerer. We'll use this to select for OpenSea transactions on Seaport.
  from iv_nft_trades
;