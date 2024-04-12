{{ config(
    schema = 'contracts_optimism',
    alias = 'disperse_contracts'
    )
}}

-- Pull Disperse Contracts
-- Disperse contracts often function where tokens are sent to the disperser then distributed to users

WITH disperse_contracts AS (
    SELECT address AS contract_address, contract_name
    FROM (values
             (0xd152f549545093347a162dce210e7293f1452150,'Disperse') --generic disperse
            ,(0x52da4de336f8354f7b33a472bf010cd4a3b640ae,'Disperse') --generic disperse
            ,(0x50a64d05bb8618d8d96a83cbbb12b3044ec3489a,'Disperse') --generic disperse
            ,(0x77fb265dac16ccc5cc80c3583a72cf7776f9b759,'Disperse') --generic disperse

            ,(0xbd39115fc389b9a063af22679b49d035e1f1de58,'MultiSend') --generic multisend
            ,(0xdd29ddac5e6ada1359fc20b8debad2b98963e0dd,'MultiSend') --generic multisend
            ,(0x714dc96eb217b511a882b6c472d106620ec5a4d2,'MultiSend') --generic multisend

            ,(0xbe9a9b1b07f027130e56d8569d1aea5dd5a86013,'OP Airdrop 2 Distributor') --OP Airdrop 2 disperse
        ) a (address, contract_name)
    )

SELECT
    contract_address, contract_name

 FROM disperse_contracts