BEGIN;
DROP VIEW IF EXISTS qidao."view_evt_transfer" CASCADE;

CREATE VIEW qidao."view_evt_transfer" AS(
    --transfer of vault NFT from one address to another
    select
        CASE WHEN "interaction_type" = 'base' then NULL else "tokenId" END as vaultID,
        contract_address as contract_address,
        evt_tx_hash as evt_tx_hash,
        evt_index as evt_index,
        evt_block_time as evt_block_time,
        evt_block_number as evt_block_number,
        "to" as address_one,
        "from" as address_two,
        'to' as address_one_type,
        'from' as address_two_type,
        NULL::numeric as amount_mai,
        NULL::numeric as amount_collateral,
        NULL::numeric as closing_fee,
        NULL::bool as approved_bool,
        "tokenId" as TokenId,
        'transfer' as transaction_type,
        interaction_type as interaction_type
    from
        ((select
            *,
            'crosschain' as interaction_type
        from
            qidao."crosschainQiStablecoin_evt_Transfer"
        )
        union 
        (select
            *,
            'crosschainV2' as interaction_type
        from
            qidao."CrosschainQiStablecoinV2_evt_Transfer"
        )
        union
        (select
            *,
            'erc20' as interaction_type
        from
            qidao."erc20QiStablecoin_evt_Transfer"
        )
        union 
        (select
            *,
            'erc20' as interaction_type
        from
            qidao."erc20QiStablecoinwbtc_evt_Transfer"
        )
        union 
        (select
            *,
            'base' as interaction_type
        from
            qidao."QiStablecoin_evt_Transfer"
        )) transfer
    );

COMMIT;
