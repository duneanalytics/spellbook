{{config(
        schema='coinbase_wallet_quests_optimism',
        alias=alias('distributor_addresses'),
        tags = ['dunesql'],
        post_hook='{{ expose_spells(\'["optimism"]\',
                                    "project",
                                    "coinbase_wallet_quests",
                                    \'["msilb7"]\') }}')}}


{% set op_token = 0x4200000000000000000000000000000000000042 %}

WITH quest_addresses AS (
SELECT distributor_address
    , rewards_token
    , quest_name
FROM (values
    --  (0x9fFD84fA80932Fa55E761B06398aA2577815c459,'Admin & Gas Fee') --ignore
     (0xf42279467D821bCDf40b50E9A5d2cACCc4Cf5b30,'{{op_token}}','Quest 1 - DEX')
    ,(0x9F4F2B8BdA8D2d3832021b3119747470ea86A183,'{{op_token}}','Quest 2 - Delegation')
    ,(0x1fe95e0497a0E38AFBE18Bd19B9a2b42116880f0,'{{op_token}}','Quest 3 - Attestation')
    ,(0x12d9aEF514EE8Bc3f7B2d523ae26164632b71acB,'{{op_token}}','Quest 4 - Deposit')
    ,(0x0b1CEf4cAb511426B001f430b8d000D5a8C83AD0,'{{op_token}}','Quest 5 - Mint NFT')
        
    ) a (distributor_address, rewards_token, quest_name)

)

SELECT 
    distributor_address, rewards_token, quest_name
    FROM quest_addresses
    group by 1,2,3