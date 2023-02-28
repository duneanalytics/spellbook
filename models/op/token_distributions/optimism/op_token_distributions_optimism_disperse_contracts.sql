{{ config(
    schema = 'op_token_optimism',
    alias = 'disperse_contracts'
    )
}}

-- Pull Disperse Contracts
-- Disperse contracts often function where tokens are sent to the disperser then distributed to users

WITH disperse_contracts AS (
    SELECT LOWER(address) AS address, name_override
    FROM (values
             ('0xd152f549545093347a162dce210e7293f1452150',NULL) --generic disperse
            ,('0x5bc45d36577df70a7865c1d8af47cdf7db3efbd8','OP Airdrop 2 Distributor') --OP Airdrop 2 disperse
        ) a (address, name_override)
    )

SELECT
address, 'Utility' as label, name_override AS address_descriptor

 FROM disperse_contracts