{{ config (
    alias = alias('vaults'),
    post_hook = '{{ expose_spells(\'["ethereum"]\', "project", "tessera",\'["amadarrrr"]\') }}'
) }}
-- VAULT DEPLOY
SELECT
    _deployer AS deployer,
    _origin AS origin,
    _owner AS owner,
    _vault AS vault,
    evt_block_time AS block_time,
    evt_tx_hash AS tx_hash
FROM
    {{ source('tessera_ethereum','VaultFactory_evt_DeployVault') }};
