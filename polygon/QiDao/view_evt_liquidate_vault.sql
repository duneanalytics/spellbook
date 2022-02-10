BEGIN;

DROP VIEW IF EXISTS qidao."view_evt_liquidate_vault" CASCADE;
CREATE VIEW qidao."view_evt_liquidate_vault" AS(
  select
      "vaultID" as vaultID,
      contract_address as contract_address,
      evt_tx_hash as evt_tx_hash,
      evt_index as evt_index,
      evt_block_time as evt_block_time,
      evt_block_number as evt_block_number,
      buyer as address_one,
      owner as address_two,
      'buyer' as address_one_type,
      'owner' as address_two_type,
      "debtRepaid"/10^18 * -1 as amount_mai,
      (CASE when contract_address in ('\x7dda5e1a389e0c1892caf55940f5fce6588a9ae0','\x37131aedd3da288467b6ebe9a77c523a700e6ca1') then "collateralLiquidated"/10^8
          else "collateralLiquidated"/10^18
      END * -1) as collateral_liquidated,
      (CASE when contract_address in ('\x7dda5e1a389e0c1892caf55940f5fce6588a9ae0','\x37131aedd3da288467b6ebe9a77c523a700e6ca1') then "closingFee"/10^8
          else "closingFee"/10^18
      END) as closing_fee,
      NULL::bool as approved_bool,
      NULL::numeric as TokenId,
      'liquidate_vault' as transaction_type,
      interaction_type as interaction_type
  from
      ((select
          *,
          'crosschain' as interaction_type
      from
          qidao."crosschainQiStablecoin_evt_LiquidateVault"
      )
      union
      (select
          *,
          'crosschainV2' as interaction_type
      from
          qidao."CrosschainQiStablecoinV2_evt_LiquidateVault"
      )
      union
      (select
          *,
          'erc20' as interaction_type
      from
          qidao."erc20QiStablecoin_evt_LiquidateVault"
      )
      union
      (select
          *,
          'erc20' as interaction_type
      from
          qidao."erc20QiStablecoinwbtc_evt_LiquidateVault"
      )
      union
      (select
          "vaultID",
          owner,
          "buyer",
          "amountPaid" as debtRepaid,
          NULL as collateralLiquidated,
          NULL as closingFee,
          contract_address,
          evt_tx_hash,
          evt_index,
          evt_block_time,
          evt_block_number,
          'base' as interaction_type
      from
          qidao."QiStablecoin_evt_BuyRiskyVault"
      )) liquidate_vault
  );
COMMIT;
