BEGIN;
DROP VIEW IF EXISTS qidao.view_vaults_collaterals_changes CASCADE;

CREATE VIEW qidao.view_vaults_collaterals_changes AS (
with vaults_dws as
(
  select "evt_block_time", "evt_tx_hash", "contract_address", "amount"
  from qidao."erc20QiStablecoin_evt_DepositCollateral"
  union all
  select "evt_block_time", "evt_tx_hash", "contract_address",
         "amount" *(-1) as "amount"
  from qidao."erc20QiStablecoin_evt_WithdrawCollateral"
  union all
  select "evt_block_time", "evt_tx_hash", "contract_address", "amount"
  from qidao."crosschainQiStablecoin_evt_DepositCollateral"
  union all
  select "evt_block_time", "evt_tx_hash", "contract_address",
         "amount" * (-1) as "amount"
  from qidao."crosschainQiStablecoin_evt_WithdrawCollateral"
  union all
  select "evt_block_time", "evt_tx_hash", "contract_address", "amount"
  from qidao."CrosschainQiStablecoinV2_evt_DepositCollateral"
  union all
  select "evt_block_time", "evt_tx_hash", "contract_address", 
         "amount" * (-1) as "amount"
  from qidao."CrosschainQiStablecoinV2_evt_WithdrawCollateral"
  union all
  select "evt_block_time", "evt_tx_hash", "contract_address", "amount"
  from qidao."erc20QiStablecoinwbtc_evt_DepositCollateral"
  union all
  select "evt_block_time", "evt_tx_hash", "contract_address",
         "amount" * (-1) as "amount"
  from qidao."erc20QiStablecoinwbtc_evt_WithdrawCollateral"
  union all
  select "evt_block_time", "evt_tx_hash", "contract_address", "amount"
  from qidao."QiStablecoin_evt_DepositCollateral"
  union all
  select "evt_block_time", "evt_tx_hash", "contract_address",
         "amount" * (-1) as "amount"
  from qidao."QiStablecoin_evt_WithdrawCollateral"
  order by 1
)
,vaults_changes as
(
  select a."evt_block_time" as "date", b."collateral_contract",
         b."collateral_symbol", b."collateral_price_symbol",
         a."amount"/(10^b."collateral_decimals") as "change_in_collateral_amount"
  from vaults_dws a
       inner join qidao."view_vaults_collaterals_mapping" b
       on a."contract_address" = b."vault_contract"
)
select * from vaults_changes order by 1
);

COMMIT;