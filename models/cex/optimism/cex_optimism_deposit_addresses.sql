{{config(
        schema = "cex_optimism",
        tags = ['dunesql'],
        alias = alias('deposit_addresses'),
        post_hook='{{ expose_spells(\'["optimism"]\',
                                    "sector",
                                    "cex",
                                    \'["kaiblade"]\') }}')}}


WITH vault_deposits AS 
(SELECT evt_tx_hash AS tx_hash, "from" AS cex_deposit_address,  to AS cex_vault_address
FROM {{ ref('evms_erc20_transfers') }}
WHERE to IN ( SELECT address FROM {{ ref('cex_optimism_addresses') }})
AND "from" NOT IN (SELECT address FROM {{ ref('cex_optimism_addresses') }})
AND blockchain = 'optimism'

UNION 

SELECT tx_hash, tx_from AS cex_deposit_address, tx_to AS cex_vault_address
FROM {{ ref('transfers_optimism_eth') }}
WHERE tx_to IN (SELECT address FROM {{ ref('cex_optimism_addresses') }})
AND "from" NOT IN (SELECT address FROM {{ ref('cex_optimism_addresses') }})
),

deposit_addresses AS
(SELECT DISTINCT(deposits.cex_deposit_address), vaults.cex_name
FROM vault_deposits deposits
JOIN {{ ref('cex_optimism_addresses') }} vaults
ON deposits.cex_vault_address = vaults.address
)

SELECT *
FROM deposit_addresses


