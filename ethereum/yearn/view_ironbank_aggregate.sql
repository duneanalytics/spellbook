BEGIN;

DROP VIEW IF EXISTS yearn."view_ironbank_aggregate" CASCADE;
CREATE VIEW yearn."view_ironbank_aggregate" AS(
WITH borrows AS(
SELECT payer as address_one,
borrower as address_two, 
'payer' as address_one_type, 
'borrower' as address_two_type,
"repayAmount" as amount_one,
"accountBorrows" as amount_two,
'repayAmount' as amount_one_type, 
'accountBorrows' as amount_two_type, 
 NULL::bytea as c_token_collateral,
contract_address, 
evt_tx_hash, 
evt_index, 
evt_block_time, 
evt_block_number, 
'RepayBorrow' as transaction_type FROM yearn."ironbank_evt_RepayBorrow"
UNION ALL
(SELECT "borrower" as address_one,
NULL as address_two, 
'borrower' as address_one_type, 
NULL as address_two_type,
"borrowAmount" as amount_one,
"accountBorrows" as amount_two,
'borrowAmount' as amount_one_type, 
'accountBorrows' as amount_two_type, 
 NULL::bytea as c_token_collateral,
contract_address, 
evt_tx_hash, 
evt_index, 
evt_block_time, 
evt_block_number, 
'Borrow' as transaction_type
FROM yearn."ironbank_evt_Borrow"
)
UNION ALL
(SELECT admin as address_one,
NULL as address_two, 
'admin' as address_one_type, 
NULL as address_two_type,
"reduceAmount" as amount_one,
"newTotalReserves" as amount_two,
'reduceAmount' as amount_one_type, 
'newTotalReserves' as amount_two_type, 
 NULL::bytea as c_token_collateral,
contract_address, 
evt_tx_hash, 
evt_index, 
evt_block_time, 
evt_block_number, 
'ReservesReduced' as transaction_type
FROM yearn."ironbank_evt_ReservesReduced"
)


), 

transfers as (
(SELECT "from" as address_one,
"to" as address_two, 
'from' as address_one_type, 
'to' as address_two_type,
"amount" as amount_one,
NULL as amount_two,
'amount' as amount_one_type, 
NULL as amount_two_type, 
 NULL::bytea as c_token_collateral,
contract_address, 
evt_tx_hash, 
evt_index, 
evt_block_time, 
evt_block_number, 
'Transfer' as transaction_type
FROM yearn."ironbank_evt_Transfer"
)
),


two_tokens as (
(SELECT liquidator as address_one,
borrower as address_two, 
'liquidator' as address_one_type, 
'borrower' as address_two_type,
"repayAmount" as amount_one,
"seizeTokens" as amount_two,
'repayAmount' as amount_one_type, 
'number_tokens_seized' as amount_two_type, 
"cTokenCollateral" as c_token_collateral,
contract_address, 
evt_tx_hash, 
evt_index, 
evt_block_time, 
evt_block_number, 
'Liquidate' as transaction_type
FROM yearn."ironbank_evt_LiquidateBorrow"
)
UNION ALL
(SELECT minter as address_one,
NULL::bytea as address_two, 
'minter' as address_one_type, 
'none' as address_two_type,
"mintAmount" as amount_one,
"mintTokens" as amount_two,
'mintAmount' as amount_one_type, 
'mintTokens' as amount_two_type, 
 NULL::bytea as c_token_collateral,
contract_address, 
evt_tx_hash, 
evt_index, 
evt_block_time, 
evt_block_number, 
'Mint' as transaction_type
FROM yearn."ironbank_evt_Mint"
)
UNION ALL 
(SELECT redeemer as address_one,
NULL as address_two, 
'redeemer' as address_one_type, 
NULL as address_two_type,
"redeemAmount" as amount_one,
"redeemTokens" as amount_two,
'redeemAmount' as amount_one_type, 
'redeemTokens' as amount_two_type, 
 NULL::bytea as c_token_collateral,
contract_address, 
evt_tx_hash, 
evt_index, 
evt_block_time, 
evt_block_number, 
'Redeem' as transaction_type
FROM yearn."ironbank_evt_Redeem"
)
)


SELECT  
tt.address_one, 
tt.address_two, 
tt.address_one_type, 
tt.address_two_type, 
tt."c_token_collateral",
tt.amount_one/10^yct.yvault_deposit_token_decimals as amount_one,
tt.amount_two/10^yct.yvault_deposit_token_decimals as amount_two,
tt.amount_one_type, 
tt.amount_two_type, 
tt.contract_address, 
tt.evt_tx_hash, 
tt.evt_index, 
tt.evt_block_time, 
tt.evt_block_number, 
tt.transaction_type 
FROM two_tokens tt
LEFT JOIN yearn."view_yearn_contract_tokens" yct on tt.contract_address = yct.yvault_contract

LEFT JOIN erc20."tokens" tok on yct.yvault_contract = tok.contract_address
UNION ALL 
SELECT  
bo.address_one, 
bo.address_two, 
bo.address_one_type, 
bo.address_two_type, 
bo.c_token_collateral,
bo.amount_one/10^yct.yvault_deposit_token_decimals as amount_one,
bo.amount_two/10^yct.yvault_deposit_token_decimals as amount_two,
bo.amount_one_type, bo.amount_two_type, 
bo.contract_address, 
bo.evt_tx_hash, 
bo.evt_index, 
bo.evt_block_time, 
bo.evt_block_number, bo.transaction_type 
FROM borrows bo
LEFT JOIN yearn."view_yearn_contract_tokens" yct on bo.contract_address = yct.yvault_contract
UNION ALL 
SELECT  
tr.address_one, 
tr.address_two, 
tr.address_one_type, 
tr.address_two_type, 
tr.c_token_collateral,
tr.amount_one/10^tok.decimals as amount_one,
NULL::numeric as amount_two,
tr.amount_one_type, tr.amount_two_type, 
tr.contract_address, 
tr.evt_tx_hash, 
tr.evt_index, 
tr.evt_block_time, 
tr.evt_block_number, tr.transaction_type 
FROM transfers tr
LEFT JOIN erc20."tokens" tok on tr.contract_address = tok.contract_address
 )
