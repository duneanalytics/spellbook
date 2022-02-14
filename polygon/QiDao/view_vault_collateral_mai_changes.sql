BEGIN;
DROP VIEW IF EXISTS qidao.view_vault_collateral_mai_changes CASCADE;

CREATE VIEW qidao.view_vault_collateral_mai_changes AS (
select "block_time", "tx_hash", "vault_contract_address", "vault_id",
       "collateral_contract_address", "collateral_symbol",
       "collateral_price_symbol",
       "amount" as "change_in_collateral",
       0 as "change_in_issued_mai",
       0 as "fee_in_collateral",
       0 as "fee_in_usd",
       'deposit' as "tx_type"
from qidao.view_deposit_collateral
union all
select "block_time", "tx_hash", "vault_contract_address", "vault_id",
       "collateral_contract_address", "collateral_symbol",
       "collateral_price_symbol",
       "amount" * (-1) as "change_in_collateral",
       0 as "change_in_issued_mai",
       0 as "fee_in_collateral",
       0 as "fee_in_usd",
       'withdraw' as "tx_type"
from qidao.view_withdraw_collateral
union all
select "block_time", "tx_hash", "vault_contract_address", "vault_id",
       "collateral_contract_address", "collateral_symbol",
       "collateral_price_symbol",
       0 as "change_in_collateral",
       "amount" as "change_in_issued_mai",
       0 as "fee_in_collateral",
       0 as "fee_in_usd",
       'borrow' as "tx_type"
from qidao.view_borrow_mai
union all
select "block_time", "tx_hash", "vault_contract_address", "vault_id",
       "collateral_contract_address", "collateral_symbol",
       "collateral_price_symbol",
       "fee_in_collateral" * (-1) as "change_in_collateral",
       "amount" * (-1) as "change_in_issued_mai",
       "fee_in_collateral",
       "fee_in_usd",
       'payback' as "tx_type"
from qidao.view_payback_mai
union all
select "block_time", "tx_hash", "vault_contract_address", "vault_id",
       "collateral_contract_address", "collateral_symbol",
       "collateral_price_symbol",
       ("collateral_liquidated" + "fee_in_collateral") * (-1) as "change_in_collateral",
       "mai_repaid" * (-1) as "change_in_issued_mai",
       "fee_in_collateral",
       "fee_in_usd",
       'liquidate' as "tx_type"
from qidao.view_liquidate_vault
order by 1
);

COMMIT;