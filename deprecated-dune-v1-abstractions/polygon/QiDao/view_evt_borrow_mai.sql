BEGIN;

DROP VIEW IF EXISTS qidao."view_evt_borrow_mai" CASCADE;
CREATE VIEW qidao."view_evt_borrow_mai" AS(
      select
          "vaultID" as vaultID,
          contract_address as contract_address,
          evt_tx_hash as evt_tx_hash,
          evt_index as evt_index,
          evt_block_time as evt_block_time,
          evt_block_number as evt_block_number,
          NULL::bytea as address_one,
          NULL::bytea as address_two,
          NULL::text as address_one_type,
          NULL::text as address_two_type,
          "amount"/10^18 as amount_mai,
          NULL::numeric as amount_collateral,
          NULL::numeric as closing_fee,
          NULL::bool as approved_bool,
          NULL::numeric as TokenId,
          'borrow_mai' as transaction_type,
          interaction_type as interaction_type
      from
          ((select
              *,
              'crosschain' as interaction_type
          from
              qidao."crosschainQiStablecoin_evt_BorrowToken"
          )
          union
          (select
              *,
              'crosschainV2' as interaction_type
          from
              qidao."CrosschainQiStablecoinV2_evt_BorrowToken"
          )
          union
          (select
              *,
              'erc20' as interaction_type
          from
              qidao."erc20QiStablecoin_evt_BorrowToken"
          )
          union
          (select
              *,
              'erc20' as interaction_type
          from
              qidao."erc20QiStablecoinwbtc_evt_BorrowToken"
          )
          union
          (select
              *,
              'base' as interaction_type
          from
              qidao."QiStablecoin_evt_BorrowToken"
          )) borrow_token
      );

COMMIT;
