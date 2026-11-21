{{ config(
    alias = 'squeeze'
    , post_hook = '{{ hide_spells() }}'
) }}

-- Static identifier labels for Squeeze launchpad contracts / wallets.
-- Source of truth: Squeeze utils/squeezeIntegration.js
-- Note: Airlock contracts are shared across Doppler integrators — labels mark
-- the contracts Squeeze uses; launch attribution still requires integrator filter.

SELECT blockchain, address, name, category, contributor, source, created_at, updated_at, model_name, label_type
FROM (
    VALUES
        ('base', 0x6C61feE73584670AbEd65101946734006DAB12d6, 'Squeeze: Integrator / Platform Fee Wallet', 'infrastructure', 'flockain', 'static', TIMESTAMP '2026-07-25', now(), 'labels_squeeze', 'identifier')
        , ('robinhood', 0x6C61feE73584670AbEd65101946734006DAB12d6, 'Squeeze: Integrator / Platform Fee Wallet', 'infrastructure', 'flockain', 'static', TIMESTAMP '2026-07-25', now(), 'labels_squeeze', 'identifier')
        , ('base', 0x660eAaEdEBc968f8f3694354FA8EC0b4c5Ba8D12, 'Doppler Airlock (Base; Squeeze uses integrator filter)', 'infrastructure', 'flockain', 'static', TIMESTAMP '2026-07-25', now(), 'labels_squeeze', 'identifier')
        , ('robinhood', 0xeb7c034704ef8dcd2d32324c1545f62fb4ad0862, 'Doppler Airlock (Robinhood; Squeeze uses integrator filter)', 'infrastructure', 'flockain', 'static', TIMESTAMP '2026-07-25', now(), 'labels_squeeze', 'identifier')
        , ('base', 0x65de470da664a5be139a5d812be5fda0d76cc951, 'Squeeze fee source: Base Multicurve Initializer', 'infrastructure', 'flockain', 'static', TIMESTAMP '2026-07-25', now(), 'labels_squeeze', 'identifier')
        , ('robinhood', 0x4e3468951d49f2eea976ed0d6e75ffcb44a9a544, 'Squeeze fee source: Robinhood Hook Initializer', 'infrastructure', 'flockain', 'static', TIMESTAMP '2026-07-25', now(), 'labels_squeeze', 'identifier')
) AS t (blockchain, address, name, category, contributor, source, created_at, updated_at, model_name, label_type)

UNION ALL

SELECT
    'solana' AS blockchain
    , from_base58('FpKUW9vDSRPTByNu4MerR2SU4YPkJU9pLWQTnChGAW3h') AS address
    , 'Squeeze: LaunchLab PlatformConfig PDA' AS name
    , 'infrastructure' AS category
    , 'flockain' AS contributor
    , 'static' AS source
    , TIMESTAMP '2026-07-25' AS created_at
    , now() AS updated_at
    , 'labels_squeeze' AS model_name
    , 'identifier' AS label_type

UNION ALL

SELECT
    'solana' AS blockchain
    , from_base58('2qUg6a3yCSATL7stUyJHDBgFJwLW8DXzemZQDePxscws') AS address
    , 'Squeeze: LaunchLab Claim / Platform Admin Wallet' AS name
    , 'infrastructure' AS category
    , 'flockain' AS contributor
    , 'static' AS source
    , TIMESTAMP '2026-07-25' AS created_at
    , now() AS updated_at
    , 'labels_squeeze' AS model_name
    , 'identifier' AS label_type
