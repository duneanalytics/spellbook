BEGIN;

DROP VIEW IF EXISTS qidao."view_evt_payback_mai" CASCADE;
CREATE VIEW qidao."view_evt_payback_mai" AS(
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
        ("amount"/10^18) * -1 as amount_mai,
        NULL::numeric as amount_collateral,
        (CASE when contract_address in ('\x7dda5e1a389e0c1892caf55940f5fce6588a9ae0','\x37131aedd3da288467b6ebe9a77c523a700e6ca1') then "closingFee"/10^8
        else "closingFee"/10^18
        END) as closing_fee,
        NULL::bool as approved_bool,
        NULL::numeric as TokenId,
        'payback_mai' as transaction_type,
        interaction_type as interaction_type
    from
        ((select
            *,
            'crosschain' as interaction_type
        from
            qidao."crosschainQiStablecoin_evt_PayBackToken"
        )
        union
        (select
            *,
            'crosschainV2' as interaction_type
        from
            qidao."CrosschainQiStablecoinV2_evt_PayBackToken"
        )
        union
        (select
            *,
            'erc20' as interaction_type
        from
            qidao."erc20QiStablecoin_evt_PayBackToken"
        )
        union
        (select
            *,
            'erc20' as interaction_type
        from
            qidao."erc20QiStablecoinwbtc_evt_PayBackToken"
        )
        union
        (select
            *,
            'base' as interaction_type
        from
            qidao."QiStablecoin_evt_PayBackToken"
        )) payback_token
    );

COMMIT;
