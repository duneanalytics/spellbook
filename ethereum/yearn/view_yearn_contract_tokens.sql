
BEGIN;
DROP VIEW IF EXISTS yearn."view_yearn_contract_tokens" cascade;

CREATE VIEW yearn."view_yearn_contract_tokens" AS(
    SELECT
    DISTINCT ON ("from")
    ett."mod_contract" AS yVault_deposit_token,
    et."decimals" AS yVault_deposit_token_decimals,
    et."symbol" AS yVault_deposit_token_symbol,
    ett."from" AS yVault_contract,
    CASE
        WHEN "from" IN (SELECT DISTINCT("contract_address") FROM yearn."yVault_evt_Transfer") then 'yearn_v1'
        WHEN "from" IN (SELECT DISTINCT("contract_address") yearn_type FROM iearn_v2."yToken_evt_Transfer") then 'iearn_v2'
        WHEN "from" IN (SELECT DISTINCT("contract_address") as yearn_type FROM yearn_v2."yVault_evt_Transfer") then 'yearn_v2'
        WHEN "from" IN (SELECT DISTINCT("contract_address") AS yearn_type FROM yearn."ironbank_evt_Transfer") THEN 'ironbank'
        WHEN "from" IN (SELECT DISTINCT("contract_address") AS yearn_type FROM yearn."woofy_evt_Transfer") THEN 'woofy'
    END AS yearn_type,
    COUNT(*) AS count_withdrawals
    FROM
    (
    SELECT
        CASE
            WHEN "from" = '\x16de59092dae5ccf4a1e6439d611fd0653f0bd01'::bytea THEN '\x6b175474e89094c44da98b954eedeac495271d0f'::bytea
            WHEN "from" = '\xd6ad7a6750a7593e092a9b218d66c0a814a3436e'::bytea THEN '\xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48'::bytea
            WHEN "from" = '\x83f798e925bcd4017eb265844fddabb448f1707d'::bytea THEN '\xdac17f958d2ee523a2206206994597c13d831ec7'::bytea
            WHEN "from" = '\xf61718057901f84c4eec4339ef8f0d86d2b45600'::bytea THEN '\x57ab1ec28d129707052df4df418d58a2d46d5f51'::bytea
            WHEN "from" = '\x73a052500105205d34daf004eab301916da8190f'::bytea THEN '\x0000000000085d4780b73119b644ae5ecd22b376'::bytea
            WHEN "from" = '\x04aa51bbcb46541455ccf1b8bef2ebc5d3787ec9'::bytea THEN '\x2260fac5e5542a773aa44fbcfedf7c193bc2c599'::bytea
            WHEN "from" = '\xc2cb1040220768554cf699b0d863a3cd4324ce32'::bytea THEN '\x6b175474e89094c44da98b954eedeac495271d0f'::bytea
            WHEN "from" = '\xe6354ed5bc4b393a5aad09f21c46e101e692d447'::bytea THEN '\xdac17f958d2ee523a2206206994597c13d831ec7'::bytea
            WHEN "from" = '\x04bc0ab673d88ae9dbc9da2380cb6b79c4bca9ae'::bytea THEN '\x4fabb145d64652a948d72533023f6e7a623c7c53'::bytea
            WHEN "from" = '\x26ea744e5b887e5205727f55dfbe8685e3b21951'::bytea THEN '\xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48'::bytea
        ELSE "contract_address" END AS mod_contract,
        "contract_address",
        "from"
    FROM
    erc20."ERC20_evt_Transfer") ett LEFT JOIN erc20."tokens" et ON ett."mod_contract" = et."contract_address"
    WHERE
    "from" IN (
        (SELECT DISTINCT("contract_address") AS yearn_type FROM yearn."yVault_evt_Transfer")
        UNION ALL
        (SELECT DISTINCT("contract_address") AS yearn_type FROM iearn_v2."yToken_evt_Transfer")
        UNION ALL
        (SELECT DISTINCT("contract_address") AS yearn_type FROM yearn_v2."yVault_evt_Transfer")
        UNION ALL
        (SELECT DISTINCT("contract_address") AS yearn_type FROM yearn."ironbank_evt_Transfer")
        --iron bank can have ibEUR,ibAUD etc as supply assets https://app.ib.xyz/
        UNION ALL
        (SELECT DISTINCT("contract_address") AS yearn_type FROM yearn."woofy_evt_Transfer")
        )
        AND "from" != ett."contract_address"
    GROUP BY 1, 2, 3, 4, 5
    ORDER BY 4, 5 DESC
);
COMMIT;