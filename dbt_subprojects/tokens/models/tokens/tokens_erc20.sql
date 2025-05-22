{{
    config(
        schema = 'tokens'
        ,alias = 'erc20'
        ,materialized = 'view'
        ,post_hook='{{ expose_spells(\'[
                                        "arbitrum"
                                        ,"avalanche_c"
                                        ,"base"
                                        ,"blast"
                                        ,"bnb"
                                        ,"boba"
                                        ,"celo"
                                        ,"corn"
                                        ,"ethereum"
                                        ,"fantom"
                                        ,"fuse"
                                        ,"gnosis"
                                        ,"goerli"
                                        ,"ink"
                                        ,"abstract"
                                        ,"kaia"
                                        ,"lens"
                                        ,"linea"
                                        ,"mantle"
                                        ,"nova"
                                        ,"optimism"
                                        ,"ronin"
                                        ,"plume"
                                        ,"polygon"
                                        ,"scroll"
                                        ,"sei"
                                        ,"sepolia"
                                        ,"shape"
                                        ,"worldchain"
                                        ,"zkevm"
                                        ,"zksync"
                                        ,"zora"
                                        ,"bob"
                                        ,"sonic"
                                        ,"sophon"
                                        ,"berachain"
                                        ,"apechain"
                                        ,"opbnb"
                                        ,"unichain"
                                    ]\',
                                    "sector",
                                    "tokens",
                                    \'["bh2smith","0xManny","hildobby","soispoke","dot2dotseurat","mtitus6","wuligy","lgingerich","0xRob","jeff-dude","viniabussafi","IrishLatte19","angus_1","Henrystats","rantum", "IrishLatte19", "captncrunch"]\') }}'
    )
}}


select 
    blockchain
    ,contract_address
    ,symbol
    ,name
    ,decimals   
from {{source('tokens_v2', 'erc20')}}
union all
select 
    blockchain
    ,contract_address
    ,symbol
    ,symbol as name
    ,decimals
from (
    select * from {{ref('tokens_v1_erc20')}}
    where blockchain not in (select distinct blockchain from {{source('tokens_v2', 'erc20')}})
)
