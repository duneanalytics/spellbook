BEGIN;

DROP VIEW IF EXISTS qidao."view_evt_aggregate" CASCADE;
CREATE VIEW qidao."view_evt_aggregate" AS(
      (select
          *
      from
          qidao."view_evt_approval"
      )
      UNION ALL
      (select
          *
      from
          qidao."view_evt_create_vault"
      )
      UNION ALL
      (select
          *
      from
          qidao."view_evt_transfer"
      )
      UNION ALL
      (select
          *
      from
          qidao."view_evt_destroy_vault"
      )
      UNION ALL
      (select
          *
      from
          qidao."view_evt_transfer_vault"
      )
      UNION ALL
      (select
          *
      from
          qidao."view_evt_ownership_transfer"
      )
      UNION ALL
      (select
          *
      from
          qidao."view_evt_liquidate_vault"
      )
      UNION ALL
      (select
          *
      from
          qidao."view_evt_payback_mai"
      )
      UNION ALL
      (select
          *
      from
          qidao."view_evt_borrow_mai"
      )
      UNION ALL
      (select
          *
      from
          qidao."view_evt_withdraw_collateral"
      )
      UNION ALL
      (select
          *
      from
          qidao."view_evt_deposit_collateral"
      )
  );
COMMIT;
