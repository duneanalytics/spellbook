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
),

-- address frequency of more than 1 means it's not a CEX deposit address
deposit_addresses_frequency AS
(SELECT cex_deposit_address, COUNT(cex_deposit_address) AS address_frequency
FROM deposit_addresses
GROUP BY cex_deposit_address
)

SELECT *
FROM deposit_addresses
WHERE cex_deposit_address NOT IN (SELECT cex_deposit_address FROM deposit_addresses_frequency WHERE address_frequency > 1 )



