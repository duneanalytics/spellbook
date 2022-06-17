-- Manually checking ERC1155 balances on Zerion/DappRadar/Zaper on June 17th 2022
-- https://dappradar.com/hub/wallet/eth/0x09a5943a6d10919571ee2c9f63380aea747eca97/nfts-financial
-- Check Dune query on v2 here: https://dune.com/queries/919512?d=1 
with sampled_wallets as
 (
     select *
     from {{ ref('balances_ethereum_erc1155_june17th') }}
 )

, unit_tests as
(
    select case when bal_day.amount = sampled_wallets.amount then true else False end as amount_test
     from {{ ref('balances_ethereum_erc1155_day') }} bal_day
     JOIN sampled_wallets ON sampled_wallets.wallet_address = bal_day.wallet_address 
     AND sampled_wallets.token_address = bal_day.token_address
     AND sampled_wallets.tokenid = bal_day.tokenId
     where bal_day.wallet_address = lower('0x09a5943a6d10919571eE2C9F63380aEA747ECA97')
     AND day = '2022-06-17')

select count(case when amount_test = false then 1 else null end)/count(*) as pct_mismatch, count(*) as count_rows
from unit_tests
-- Having mismatches less than 1% of rows
having count(case when amount_test = false then 1 else null end) > count(*)*0.01