BEGIN;
DROP VIEW IF EXISTS qidao."view_contract_token_label" CASCADE;

CREATE VIEW qidao."view_contract_token_label" AS(
SELECT 
    cp."qidao_contract",
    cp."token_contract" as collateral_token_contract,
    cp."token_symbol" as collateral_token_symbol,
    cp."count_txs",
    case when cp."token_contract" = '\x0470cd31c8fcc42671465880ba81d631f0b76c1d'::bytea then '\x7ceb23fd6bc0add59e62ac25578270cff1b9f619'::bytea --camWETH -> WETH
    when cp."token_contract" = '\x7068ea5255cb05931efa8026bd04b18f3deb8b0b'::bytea then '\x0d500b1d8e8ef31e21c99d1db9a6444d3adf1270'::bytea --camWMATIC -> WMATIC
    when cp."token_contract" = '\xea4040b21cb68afb94889cb60834b13427cfc4eb'::bytea then '\xd6df932a45c0f255f85145f286ea0b292b21c90b'::bytea -- camAAVE -> POS(AAVE)
    when cp."token_contract" = '\x7d60f21072b585351dfd5e8b17109458d97ec120'::bytea then '\x2791bca1f2de4661ed88a30c99a7a9449aa84174'::bytea -- sdam3CRV -> USDC
    when cp."token_contract" = '\xe6c23289ba5a9f0ef31b8eb36241d5c800889b7b'::bytea then '\x2791bca1f2de4661ed88a30c99a7a9449aa84174'::bytea --camDAI -> USDC
    when cp."token_contract" = '\xba6273a78a23169e01317bd0f6338547f869e8df'::bytea then '\x1bfd67037b42cf73acf2047067bd4f2c47d9bfd6'::bytea --camWBTC -> WBTC
    else cp."token_contract" end as price_address
FROM
    (SELECT
        qa."contract_address" as qidao_contract,
        et."contract_address" as token_contract,
        tok."symbol" as token_symbol,
        count(qa."contract_address") as count_txs
    from
        qidao."view_evt_aggregate" qa
        left join erc20."ERC20_evt_Transfer" et on qa."evt_tx_hash" = et."evt_tx_hash"
        left join erc20."tokens" tok on et."contract_address" = tok."contract_address"
    where
        transaction_type = 'withdraw_collateral'
        and (CASE when qa.contract_address in ('\x7dda5e1a389e0c1892caf55940f5fce6588a9ae0','\x37131aedd3da288467b6ebe9a77c523a700e6ca1') then (et."value"/10^8)*-1
            else (et."value"/10^18)*-1
            END
            ) = qa."amount_collateral"
    group by 1,2,3
    order by 1) cp inner join (
    SELECT
    "qidao_contract",
    MAX("count_txs") as max_count
    FROM
    (SELECT
        qa."contract_address" as qidao_contract,
        et."contract_address" as token_contract,
        tok."symbol" as token_symbol,
        count(qa."contract_address") as count_txs
    from
        qidao."view_evt_aggregate" qa
        left join erc20."ERC20_evt_Transfer" et on qa."evt_tx_hash" = et."evt_tx_hash"
        left join erc20."tokens" tok on et."contract_address" = tok."contract_address"
    where
        transaction_type = 'withdraw_collateral'
        and (CASE when qa.contract_address in ('\x7dda5e1a389e0c1892caf55940f5fce6588a9ae0','\x37131aedd3da288467b6ebe9a77c523a700e6ca1') then (et."value"/10^8)*-1
            else (et."value"/10^18)*-1
            END
            ) = qa."amount_collateral"
    group by 1,2,3
    order by 1) col
    group by 1
    ) cp2 on cp."qidao_contract" = cp2."qidao_contract" and cp."count_txs" = cp2."max_count"
    order by "count_txs" desc
);

COMMIT;
