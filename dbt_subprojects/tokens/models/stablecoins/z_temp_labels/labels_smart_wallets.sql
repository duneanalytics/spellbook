-- Query ID: 5958245
-- Name: stablecore - labels - smart wallets

--header (incl.project code, brief desc, change log)

/*
- references:
    safe: https://github.com/safe-global/safe-deployments/tree/main/src/assets
    account abstraction erc4337: https://github.com/eth-infinitism/account-abstraction/releases
    biconomy: https://github.com/bcnmy/docs/blob/master/docs/contracts.md
    alchemy: https://www.alchemy.com/docs/wallets/smart-contracts/deployed-addresses
    dune: tbc
- remarks:
    - a safe wallet can later become ERC-4337; in such a case, field <name> will be 'erc4337' and not 'safe' anymore
    - other wallets such as Coinbase Wallet, etc are using the standard ERC4337 Entry Point, so they are included
- @todo:
    include other contract versions and chains for alchemy factory
    safe's 1.3.0 0xC22834581EbC8527d974F8a1c97E1bEA4EF910BC missing?
    what is event gnosis_safe_ethereum.safeproxyfactory_v1_5_0_evt_proxycreationl2 ?
    table <labels.safe>
    other smart account vendors: Biconomy, Light Account (Alchemy), Kernel (eroDev)...
*/


with
    chains as (
        select distinct chain from dune.stablecore.result_stablecore_tokens
    ),
    safe_factory as (
        -- v1.0.0
        select 'ethereum' as chain, proxy as addr
        from gnosis_safe_ethereum.safeproxyfactory_v1_0_0_evt_proxycreation
        union all
        -- v1.1.1
        select 'ethereum' as chain, proxy as addr
        from gnosis_safe_ethereum.safeproxyfactory_v1_1_1_evt_proxycreation
        union all
        -- v.1.3.0
        select chain, proxy as addr
        from gnosis_safe_multichain.gnosissafeproxyfactory_v1_3_0_evt_proxycreation
        join chains using (chain)
        union all
        -- v.1.4.1
        select chain, proxy as addr
        from gnosis_safe_multichain.safeproxyfactory_v_1_4_1_evt_proxycreation
        join chains using (chain)
        union all
        -- v.1.5.0
        select chain, proxy as addr
        from gnosis_safe_multichain.safeproxyfactory_v1_5_0_evt_proxycreation
        join chains using (chain)
    ),
    erc4337_factory as (
        -- v0.5
        select chain, sender as addr
        from erc4337_multichain.entrypoint_v0_5_evt_accountdeployed
        join chains using (chain)
        union all
        -- v0.6
        select chain, sender as addr
        from erc4337_multichain.entrypoint_v0_6_evt_accountdeployed
        join chains using (chain)
        union all
        -- v0.7
        select chain, sender as addr
        from erc4337_multichain.entrypoint_v0_7_evt_accountdeployed
        join chains using (chain)
        union all
        -- v0.8
        select 'ethereum' as chain, sender as addr
        from erc4337_ethereum.entrypoint_v0_8_evt_accountdeployed
    ),
    -- for pre-deployed ERC-4337 accounts created without EntryPoint (so no AccountDeployed event)
    erc4337_ops as (
        -- v0.5
        select distinct chain, sender as addr
        from erc4337_multichain.entrypoint_v0_5_evt_useroperationevent
        join chains using (chain)
        where success
        union all
        -- v0.6
        select distinct chain, sender as addr
        from erc4337_multichain.entrypoint_v0_6_evt_useroperationevent
        join chains using (chain)
        where success
        union all
        -- v0.7
        select distinct chain, sender as addr
        from erc4337_multichain.entrypoint_v0_7_evt_useroperationevent
        join chains using (chain)
        where success
        union all
        -- v0.8
        select distinct 'ethereum' as chain, sender as addr
        from erc4337_ethereum.entrypoint_v0_8_evt_useroperationevent
        where success
    ),
    biconomy_factory as (
        -- v1
        select chain, account as addr
        from biconomy_multichain.smartaccountfactory_evt_accountcreation
        join chains using (chain)
        union all
        -- v2
        select chain, account as addr
        from biconomy_multichain.smartaccountfactory_v2_evt_accountcreation
        join chains using (chain)
    ),
    alchemy_factory as (
        -- v2
        select 'ethereum' as chain, account as addr
        from splits_ethereum.passthroughwalletfactory_evt_accountcreated
        -- @TODO: include other contract versions and chains
    ),
    argent_factory as (
        -- v1
        select 'ethereum' as chain, wallet as addr
        from argent_ethereum.walletfactory_evt_walletcreated
        union all
        -- v2
        select 'ethereum' as chain, wallet as addr
        from argent_ethereum.walletfactory_v2_evt_walletcreated
        union all
        -- v3
        select 'ethereum' as chain, wallet as addr
        from argent_ethereum.walletfactory_v3_evt_walletcreated
        union all
        -- v4
        select 'ethereum' as chain, wallet as addr
        from argent_ethereum.walletfactory_v4_evt_walletcreated
    ),
    all_factories as (
        select
            chain,
            addr as contract_address,
            category,
            min(name) as name
        from (
            select chain, addr, 'smart wallet' as category, 'safe' as name from safe_factory
            union all
            select chain, addr, 'smart wallet' as category, 'erc4337' as name from erc4337_factory
            union all
            select chain, addr, 'smart wallet' as category, 'erc4337' as name from erc4337_ops
            union all
            select chain, addr, 'smart wallet' as category, 'biconomy' as name from biconomy_factory
            union all
            select chain, addr, 'smart wallet' as category, 'alchemy' as name from alchemy_factory
            union all
            select chain, addr, 'smart wallet' as category, 'argent' as name from argent_factory
        )
        group by 1,2,3
    ),
    check as (
        select name, count(distinct contract_address) as num from all_factories group by 1
    )

-- select * from check
select * from all_factories