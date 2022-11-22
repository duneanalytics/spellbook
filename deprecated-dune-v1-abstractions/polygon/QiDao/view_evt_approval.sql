BEGIN;

DROP VIEW IF EXISTS qidao."view_evt_approval" CASCADE;
CREATE VIEW qidao."view_evt_approval" AS(

    select
    *
    from
    ((select
        NULL::int as vaultID,
        contract_address as contract_address,
        evt_tx_hash as evt_tx_hash,
        evt_index as evt_index,
        evt_block_time as evt_block_time,
        evt_block_number as evt_block_number,
        owner as address_one,
        operator as address_two,
        'owner' as address_one_type,
        'operator' as address_two_type,
        NULL::numeric as amount_mai,
        NULL::numeric as amount_collateral,
        NULL::numeric as closing_fee,
        "approved" as approved_bool,
        NULL::numeric as TokenId,
        'approval_for_all' as transaction_type,
        interaction_type as interaction_type
    from
        ((select
            *,
            'crosschain' as interaction_type
        from
            qidao."crosschainQiStablecoin_evt_ApprovalForAll"
        )
        union
        (select
            *,
            'crosschainV2' as interaction_type
        from
            qidao."CrosschainQiStablecoinV2_evt_ApprovalForAll"
        )
        union
        (select
            *,
            'erc20' as interaction_type
        from
            qidao."erc20QiStablecoin_evt_ApprovalForAll"
        )
        union
        (select
            *,
            'erc20' as interaction_type
        from
            qidao."erc20QiStablecoinwbtc_evt_ApprovalForAll"
        )) approvals_for_all
    )
    union all
    (select
        NULL::int as vaultID,
        contract_address as contract_address,
        evt_tx_hash as evt_tx_hash,
        evt_index as evt_index,
        evt_block_time as evt_block_time,
        evt_block_number as evt_block_number,
        owner as address_one,
        approved as address_two,
        'owner' as address_one_type,
        'operator' as address_two_type,
        NULL::numeric as amount_mai,
        NULL::numeric as amount_collateral,
        NULL::numeric as closing_fee,
        True as approved_bool,
        NULL::numeric as TokenId,
        'approval' as transaction_type,
        interaction_type as interaction_type
    from
        ((select
            *,
            'crosschain' as interaction_type
        from
            qidao."crosschainQiStablecoin_evt_Approval"
        )
        union
        (select
            *,
            'crosschainV2' as interaction_type
        from
            qidao."CrosschainQiStablecoinV2_evt_Approval"
        )
        union
        (select
            *,
            'erc20' as interaction_type
        from
            qidao."erc20QiStablecoin_evt_Approval"
        )
        union
        (select
            *,
            'erc20' as interaction_type
        from
            qidao."erc20QiStablecoinwbtc_evt_Approval"
        )
        union
        (select
            *,
            'base' as interaction_type
        from
            qidao."QiStablecoin_evt_Approval"
        )) approvals_basic
    ))all_approvals
);

COMMIT;
