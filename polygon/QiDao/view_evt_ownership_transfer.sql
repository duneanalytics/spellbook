BEGIN;

DROP VIEW IF EXISTS qidao."view_evt_ownership_transfer" CASCADE;
CREATE VIEW qidao."view_evt_ownership_transfer" AS(
      select
          NULL::int as vaultID,
          contract_address as contract_address,
          evt_tx_hash as evt_tx_hash,
          evt_index as evt_index,
          evt_block_time as evt_block_time,
          evt_block_number as evt_block_number,
          "newOwner" as address_one,
          "previousOwner" as address_two,
          'newOwner' as address_one_type,
          'previousOwner' as address_two_type,
          NULL::numeric as amount_mai,
          NULL::numeric as amount_collateral,
          NULL::numeric as closing_fee,
          NULL::bool as approved_bool,
          NULL::numeric as TokenId,
          'ownership_transfer' as transaction_type,
          interaction_type as interaction_type
      from
          ((select
              *,
              'crosschain' as interaction_type
          from
              qidao."crosschainQiStablecoin_evt_OwnershipTransferred"
          )
          union
          (select
              *,
              'crosschainV2' as interaction_type
          from
              qidao."CrosschainQiStablecoinV2_evt_OwnershipTransferred"
          )
          union
          (select
              *,
              'erc20' as interaction_type
          from
              qidao."erc20QiStablecoin_evt_OwnershipTransferred"
          )
          union
          (select
              *,
              'erc20' as interaction_type
          from
              qidao."erc20QiStablecoinwbtc_evt_OwnershipTransferred"
          )
          union
          (select
              *,
              'base' as interaction_type
          from
              qidao."QiStablecoin_evt_OwnershipTransferred"
          )) ownership_trasfer
  );
COMMIT;
