BEGIN;

DROP VIEW IF EXISTS qidao."view_evt_create_vault" CASCADE;
CREATE VIEW qidao."view_evt_create_vault" AS(

    select
        "vaultID"::int as vaultID,
        contract_address as contract_address,
        evt_tx_hash as evt_tx_hash,
        evt_index as evt_index,
        evt_block_time as evt_block_time,
        evt_block_number as evt_block_number,
        creator::bytea as address_one,
        NULL::bytea as address_two,
        'creator'::text as address_one_type,
        NULL::text as address_two_type,
        NULL::numeric as amount_mai,
        NULL::numeric  as amount_collateral,
        NULL::numeric  as closing_fee,
        NULL::bool as approved_bool,
        NULL::numeric as TokenId,
        'create_vault' as transaction_type,
        interaction_type as interaction_type
    from
        ((select
            *,
            'crosschain' as interaction_type
        from
            qidao."crosschainQiStablecoin_evt_CreateVault"
        )
        union
        (select
            *,
            'crosschainV2' as interaction_type
        from
            qidao."CrosschainQiStablecoinV2_evt_CreateVault"
        )
        union
        (select
            *,
            'erc20' as interaction_type
        from
            qidao."erc20QiStablecoin_evt_CreateVault"
        )
        union
        (select
            *,
            'erc20' as interaction_type
        from
            qidao."erc20QiStablecoinwbtc_evt_CreateVault"
        )
        union
        (select
            *,
            'base' as interaction_type
        from
            qidao."QiStablecoin_evt_CreateVault"
        )) create_vault
);

COMMIT;
