{{config(
        schema='coinbase_wallet_quests_optimism',
        alias='distributor_addresses',
        post_hook='{{ expose_spells(\'["optimism"]\',
                                    "project",
                                    "coinbase_wallet_quests",
                                    \'["msilb7"]\') }}')}}


{% set op_token = '0x4200000000000000000000000000000000000042' %}

WITH quest_addresses AS (
SELECT lower(distributor_address) AS distributor_address, rewards_token, quest_name
FROM (values
    --  (0x9fFD84fA80932Fa55E761B06398aA2577815c459,'Admin & Gas Fee') --ignore
     ('0xf42279467D821bCDf40b50E9A5d2cACCc4Cf5b30','{{op_token}}','Quest 1 - DEX')
    ,('0x9F4F2B8BdA8D2d3832021b3119747470ea86A183','{{op_token}}','Quest 2 - Delegation')
        
    ) a (distributor_address, rewards_token, quest_name)

)

SELECT distinct 
    distributor_address, rewards_token, quest_name
    FROM quest_addresses